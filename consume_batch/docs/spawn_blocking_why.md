# spawn_blocking이 필요한 이유 — fetch_committed_offsets_by_partition

## 핵심 요약

`BaseConsumer::fetch_metadata()` / `committed()` 는 **C 라이브러리 (librdkafka) FFI 블로킹 호출**이다.
`async fn` 안에서 직접 호출하면 **tokio worker thread 자체**가 막힌다.
스케줄이 딱 하나뿐이어도 hang이 발생하는 이유가 있다. 아래에서 설명한다.

---

## 1. tokio 런타임 구조

```
#[tokio::main]   →   flavor = "multi_thread" (default)
                      worker_threads = num_cpus (보통 4~16개)
```

worker thread는 **비동기 태스크 전용** 실행 스레드다.
tokio의 핵심 규칙: **worker thread는 절대 막혀선 안 된다.**

worker thread가 막히면 그 스레드에서 실행 대기 중인 **모든 async 태스크가 멈춘다**.
tokio는 "내가 지금 blocking 당하고 있다"는 것을 알 방법이 없다. C FFI 호출이기 때문에 OS syscall처럼 yield 신호를 보내지 않는다.

---

## 2. rdkafka BaseConsumer 블로킹 호출

```rust
// offset.rs:98-105
let metadata = consumer
    .fetch_metadata(Some(&topic_owned), Duration::from_secs(10))  // 최대 10초 blocking
    .map_err(...)?;

// offset.rs:132-139
let committed_tpl = consumer
    .committed(Duration::from_secs(10))  // 최대 10초 blocking
    .map_err(...)?;
```

두 호출 모두 내부적으로 librdkafka가 Kafka broker와 TCP 통신을 하면서 응답을 기다린다.
`Duration::from_secs(10)` 은 timeout이고, 실제로는 수십ms~수초 동안 thread를 점유한다.

이것은 진짜 blocking이다:
- OS thread sleep + wakeup
- `epoll_wait` 또는 `select` 내부 루프
- tokio event loop와 완전히 분리된 동작

---

## 3. "스케줄 하나뿐인데 왜 hang이 나나?"

직관적으로 이상하게 느껴진다. 스케줄이 하나면 태스크도 하나고, worker thread 수십 개 중 하나만 막히는 건데?

실제로는 다음 요소들이 얽혀 있다.

### 3-1. tokio-cron-scheduler 내부 태스크

`tokio-cron-scheduler` 는 스케줄러 자체를 tokio 태스크로 실행한다.

```
┌─────────────────────────────────────────────────────────┐
│  tokio runtime worker threads                           │
│                                                         │
│  Thread 1: [scheduler-tick task]  →  job 발화 체크     │
│  Thread 2: [batch job task]        →  fetch_metadata() ← BLOCKED (최대 10s)
│  Thread 3: [scheduler-notify]     →  완료 대기          │
│  ...                                                    │
└─────────────────────────────────────────────────────────┘
```

scheduler 내부에는 job 실행, 완료 통보, 다음 tick 계산 등의 태스크가 있다.
batch job 태스크가 blocking 중일 때 scheduler의 내부 상태 머신도 그 결과를 기다리고 있을 수 있다.

### 3-2. worker thread 고갈 (Thread Starvation)

`num_cpus` = N 이고 현재 blocking 중인 태스크 + scheduler 내부 태스크 + 기타 태스크가 N개를 넘으면:

```
모든 worker thread가 "blocking 대기" 또는 "완료 대기" 상태로 꽉 찬다
→ tokio 런타임 자체가 진전할 수 없음
→ 프로그램 정지처럼 보임 (실제로는 10초 timeout 전까지 blocking)
```

환경에 따라 worker_threads가 적거나 (CI 서버, 컨테이너 등 `num_cpus` 가 2~4인 경우)
scheduler + DB + ES + Redis 등 여러 서비스가 동시에 초기화되어 있으면 worker thread가 금방 부족해진다.

### 3-3. blocking 중 다른 async 작업이 진전 불가

이 프로젝트에서 batch job 실행 흐름:

```
main_controller.main_task()  →  initialize_cron_scheduler()  →  scheduler.start()
                                                                       │
                                                    scheduler tick (tokio task) fires job
                                                                       │
                                              Job::new_async closure (tokio task)
                                                                       │
                                       execute_batch_by_name() → fetch_committed_offsets_by_partition()
                                                                       │
                                                          fetch_metadata() ← BLOCKING (10s)
```

`fetch_metadata()` 가 blocking 하는 동안 이 tokio task를 실행 중인 worker thread는 아무것도 못 한다.
tokio는 "이 task가 .await 하지 않는다"는 것을 인식하지 못하기 때문에 다른 ready 상태 task를 이 thread에 올리지 않는다.

---

## 4. spawn_blocking이 해결하는 방법

```rust
// offset.rs:63
let partition_offsets = tokio::task::spawn_blocking(move || {
    // fetch_metadata(), committed() 호출
    ...
}).await??;
```

`spawn_blocking` 은 tokio의 **별도 blocking thread pool**에서 클로저를 실행한다.

```
┌────────────────────────────────────────────────────────────────┐
│  tokio worker thread pool (async 전용, num_cpus개)             │
│  → async task만 처리, blocking 없음                           │
└────────────────────────────────────────────────────────────────┘
              │   spawn_blocking() 호출
              ▼
┌────────────────────────────────────────────────────────────────┐
│  tokio blocking thread pool (별도, 최대 512개 동적 생성)       │
│  → fetch_metadata() + committed() 여기서 실행                 │
│  → 이 스레드가 blocking 되어도 worker thread pool에 영향 없음 │
└────────────────────────────────────────────────────────────────┘
```

blocking thread가 작업 완료 후 결과를 channel로 worker thread에 돌려준다.
worker thread는 `.await` 으로 non-blocking 대기 → 다른 async 태스크 처리 가능.

---

## 5. 정리

| 구분 | spawn_blocking 없이 | spawn_blocking 사용 |
|------|---------------------|---------------------|
| fetch_metadata 실행 위치 | tokio worker thread | 별도 blocking thread |
| worker thread 점유 | 최대 10초 | 0 (즉시 해제) |
| 다른 async 태스크 영향 | 있음 (thread 고갈 위험) | 없음 |
| hang 가능성 | 있음 | 없음 |

**규칙: async fn 안에서 C FFI blocking 호출, 동기 파일 I/O, CPU-heavy 연산은 항상 `spawn_blocking` 으로 분리.**

---

## 6. 이 프로젝트에서 해당되는 다른 blocking 호출 패턴

`rdkafka::BaseConsumer` 의 아래 메서드들은 동일하게 blocking 이다:

| 메서드 | 위치 | blocking 여부 |
|--------|------|--------------|
| `fetch_metadata()` | offset.rs:98 | O (최대 timeout까지) |
| `committed()` | offset.rs:132 | O (최대 timeout까지) |
| `consumer.assign()` | offset.rs:124 | X (즉시 반환) |

`StreamConsumer` (비동기 consumer) 와 달리 `BaseConsumer` 는 명시적 `poll()` 기반 동기 API다.
비동기 컨텍스트에서 `BaseConsumer` 를 써야 한다면 항상 `spawn_blocking` 필수.
