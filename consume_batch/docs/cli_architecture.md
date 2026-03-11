# CLI 아키텍처 설명

## 전체 구조

이 프로젝트는 하나의 바이너리(`consume_batch_v1`)가 두 가지 모드로 실행됩니다.

```
./consume_batch_v1         → 서비스 모드 (스케줄러 + Unix 소켓 서버)
./consume_batch_v1 --cli   → CLI 클라이언트 모드 (실행 중인 서비스에 접속)
```

---

## 1. 서비스 모드 기동 흐름 (`main.rs`)

```
main()
 │
 ├─ dotenv().ok()                    // .env 파일 로드
 ├─ AppConfig::init()                // 환경변수 → 전역 설정 초기화
 ├─ set_global_logger()              // 파일 + stdout 로거 초기화
 │
 ├─ EsRepositoryImpl::new()          // Elasticsearch 연결
 ├─ MysqlRepositoryImpl::new()       // MySQL 연결
 ├─ KafkaRepositoryImpl::new()       // Kafka 연결
 │
 ├─ ElasticServiceImpl::new(es_repo)
 ├─ MysqlServiceImpl::new(mysql_repo)
 ├─ ConsumeServiceImpl::new(kafka_repo)
 ├─ ProducerServiceImpl::new(kafka_repo)
 │
 ├─ BatchServiceImpl::new(mysql, elastic, consume, producer)
 │    └─ batch_schedule.toml 로드 → BatchScheduleConfig 생성
 │
 ├─ CliServiceImpl::new(Arc<BatchService>, schedule_config)
 │
 └─ MainController::new(batch_service, Arc<cli_service>)
      └─ main_task() 진입
```

---

## 2. MainController::main_task() 내부

```
main_task()
 │
 ├─ tokio::spawn ──────────────────────────────────────────► [백그라운드]
 │       └─ cli_service.start_socket_server()
 │               └─ Unix 소켓 파일 생성 (./socket/consume_batch.sock)
 │               └─ 클라이언트 연결 대기 (loop { listener.accept() })
 │               └─ 연결 수신 시 → tokio::spawn(handle_socket_connection)
 │
 └─ batch_service.main_batch_task() ──────────────────────► [포그라운드, Ctrl+C까지 블록]
         └─ JobScheduler 생성
         └─ enabled 스케줄 중 cron_schedule_apply=true → 크론 등록
         └─ enabled 스케줄 중 immediate_apply=true → 즉시 실행
         └─ Ctrl+C 신호 대기
```

**핵심:** CLI 소켓 서버는 백그라운드 태스크로 동작하고,
배치 스케줄러는 포그라운드에서 Ctrl+C를 기다립니다.

---

## 3. CLI 클라이언트 모드 기동 흐름 (`main.rs` → `cli_client_controller.rs`)

```
main()
 │
 ├─ dotenv().ok()
 ├─ args에 "--cli" 또는 "-cli" 있으면 → CLI 모드 진입
 │    └─ SOCKET_PATH 환경변수 읽기 (없으면 ./socket/consume_batch.sock)
 │    └─ CliClientController::run(socket_path)
 │         └─ UnixStream::connect(socket_path)  // 서비스 소켓에 연결
 │         └─ loop:
 │               ├─ read_until_prompt()   // 서버 메뉴 수신 후 출력
 │               ├─ read_user_input()     // stdin에서 사용자 입력 읽기
 │               ├─ writer.write_all()    // 입력값 소켓으로 전송
 │               └─ 0 또는 q → 종료
 │
 └─ return  ← AppConfig::init() 및 서비스 초기화 없이 종료
```

**핵심:** CLI 모드는 AppConfig 전체 초기화가 필요 없습니다.
소켓 경로만 알면 서비스에 연결할 수 있습니다.

---

## 4. handle_socket_connection 상세 동작

`handle_socket_connection`은 CLI 클라이언트 1개의 연결을 담당하는 함수입니다.
`start_socket_server()`가 연결을 수락할 때마다 `tokio::spawn`으로 이 함수를 독립 태스크로 실행합니다.

### 4-1. 소켓 스트림 분리

```
UnixStream (양방향)
    │
    ├─ into_split()
    │       │
    │       ├─ OwnedReadHalf  → BufReader로 감싸서 read_line() 사용
    │       └─ OwnedWriteHalf → 소켓 writer 태스크로 이동(move)
```

`into_split()`은 스트림을 읽기/쓰기 두 소유권(Owned)으로 분리합니다.
분리된 두 half는 서로 독립적으로 다른 태스크에서 사용할 수 있습니다.

---

### 4-2. socket_tx 채널: 동시 쓰기 문제 해결

배치를 실행할 때 두 곳에서 동시에 소켓에 쓰기를 시도합니다.

- **메인 루프**: 메뉴, "Complete.", "Failed:" 등 상태 메시지를 전송
- **로그 포워딩 태스크**: 배치 실행 중 발생하는 로그를 실시간 전송

`OwnedWriteHalf`는 `&mut` 접근만 허용하므로, 두 태스크가 동시에 직접 쓸 수 없습니다.

해결책: **mpsc 채널을 통해 모든 쓰기를 단일 writer 태스크로 위임**

```
메인 루프          ──send("메뉴")──────────────►┐
로그 포워딩 태스크  ──send("[LOG] ...")──────────►├─ socket_tx (mpsc::Sender)
                                                │        │
                                            (채널)    (채널)
                                                │        │
                                            socket_rx (mpsc::Receiver)
                                                │
                                          [writer 태스크]
                                          loop { recv() → write_all() }
                                                │
                                          OwnedWriteHalf (실제 소켓 쓰기)
```

버퍼 크기 256은 배치 실행 중 로그가 빠르게 쌓여도 전송 태스크가 따라올 수 있도록 설정됩니다.

---

### 4-3. 메인 루프: 메뉴 → 입력 → 분기

```
loop {
    1. socket_tx.send(메뉴)          // "1. xxx\n2. yyy\nInput:\n"
    2. reader.read_line()            // 클라이언트 입력 대기 (블로킹)
    3. if "0" or "q" → break
    4. match 숫자 파싱 {
        유효한 번호 → 배치 실행 흐름 (4-4 참고)
        그 외      → "올바른 번호 입력하세요" 전송
       }
}
```

`read_line()`은 `\n`을 만날 때까지 대기합니다.
클라이언트가 "3\n"을 보내면 즉시 복귀합니다.

---

### 4-4. 배치 실행과 실시간 로그 스트리밍

배치를 선택했을 때의 전체 흐름입니다.

```
배치 선택 (예: "3")
    │
    ├─ 1. socket_tx.send("[spent_type] Batch execution in progress...\n")
    │
    ├─ 2. (log_tx, log_rx) = mpsc::channel(256)
    │         log_tx: 배치 실행 중 로그를 받는 송신자
    │         log_rx: 로그를 받아 소켓으로 내보내는 수신자
    │
    ├─ 3. tokio::spawn(로그 포워딩 태스크)
    │         loop {
    │             msg = log_rx.recv().await   // 로그 대기
    │             socket_tx.send("[LOG] {msg}\n")  // 소켓으로 전달
    │         }
    │         // log_tx가 drop되면 log_rx.recv()가 None → 태스크 종료
    │
    ├─ 4. CLI_LOG_TX.scope(Some(log_tx), run_batch()) 실행
    │         ↑ task-local에 log_tx 저장
    │         │
    │         └─ run_batch() → process_batch()
    │                 내부에서 batch_log!(info, "...") 호출 시:
    │                 ┌─ log::info!("...")        // 파일 + 서비스 stdout
    │                 └─ CLI_LOG_TX.try_with(...)  // log_tx로 전송 → 포워딩 태스크가 소켓으로 전달
    │
    └─ 5. scope 종료 → log_tx drop → log_rx closed → 포워딩 태스크 자동 종료
           │
           ├─ Ok(())  → socket_tx.send("[spent_type] Complete.\n")
           └─ Err(e)  → socket_tx.send("[spent_type] Failed: ...\n")
```

**task-local이란?**
`tokio::task_local!`로 선언한 변수는 특정 async 태스크 안에서만 유효한 "스레드 로컬"과 유사한 저장소입니다.
`CLI_LOG_TX.scope(value, future)`를 실행하면, 그 `future`가 실행되는 동안만 `CLI_LOG_TX`에 `value`가 설정됩니다.
`run_batch()` → `process_batch()` 전체 체인이 같은 태스크 안에서 `.await`로 실행되므로, 내부 어디서든 `CLI_LOG_TX.try_with(...)`로 sender에 접근할 수 있습니다.

크론 스케줄러가 실행하는 배치는 `tokio::spawn`으로 별도 태스크로 실행되므로,
task-local이 설정되지 않아 로그가 CLI로 전달되지 않습니다 — 의도된 동작입니다.

---

## 5. 소켓 통신 프로토콜 (업데이트)

```
[CLI 클라이언트]                          [서비스 - CliServiceImpl]
      │                                          │
      │  ←─── 메뉴 전송 (Input:\n 으로 끝남) ────    │
      │                                          │
      │  ──── "3\n" 전송 ────────────────────►    │
      │                                          ├─ "3" 파싱 → batch_items[2]
      │  ←─── "[spent_type] Batch execution..." ─│
      │                                          ├─ log 채널 생성 + 포워딩 태스크 spawn
      │                                          ├─ CLI_LOG_TX.scope(log_tx, run_batch())
      │  ←─── "[LOG] Starting batch job..." ─────│  (실시간 로그)
      │  ←─── "[LOG] Fetching 1000 rows..." ─────│  (실시간 로그)
      │  ←─── "[LOG] Indexing complete..." ───────│  (실시간 로그)
      │                                          ├─ run_batch() 완료
      │  ←─── "[spent_type] Complete.\n" ────────│
      │                                          │
      │  ←─── 다음 메뉴 전송 ───────────────────────│
```

**read_until_prompt() 동작:**
클라이언트는 `BufReader::read_line()`으로 서버 메시지를 한 줄씩 읽어 출력합니다.
`"Input:\n"`이 포함된 줄을 받으면 루프를 탈출하고 사용자 입력을 받습니다.
배치 실행 중 전달되는 `[LOG]` 줄들도 `read_until_prompt()`가 그대로 출력합니다.
(`\n` 없이 `"Input:"` 만 보내면 read_line이 영원히 대기하는 데드락 발생)

---

## 6. 로그 출력 위치

| 항목 | 위치 |
|------|------|
| 배치 실행 상세 로그 | `logs/consume_batch_v1__rCURRENT.log` (파일) |
| 배치 실행 상세 로그 | 서비스를 실행한 터미널 stdout (`duplicate_to_stdout`) |
| CLI 클라이언트 출력 | `--cli` 터미널 (메뉴, `[LOG]` 실시간 로그, "Complete.", "Failed:") |

---

## 7. 파일별 역할 요약

| 파일 | 역할 |
|------|------|
| `main.rs` | 진입점. 모드 분기 및 의존성 주입 |
| `controller/main_controller.rs` | BatchService + CliService 조합 및 태스크 실행 |
| `controller/cli_client_controller.rs` | CLI 클라이언트 (소켓 연결, 메뉴 출력, 입력 전송) |
| `service/cli_service_impl.rs` | 소켓 서버 (메뉴 전송, 입력 수신, 배치 실행) |
| `service_trait/cli_service.rs` | CliService 트레이트 정의 |
| `service/batch_service_impl.rs` | 실제 배치 로직 (MySQL→ES 인덱싱 등) |
| `app_config.rs` | 환경변수 기반 전역 설정 (SOCKET_PATH 포함) |
