# handle_socket_connection 완전 분석

`cli_service_impl.rs`의 `handle_socket_connection` 함수를 한 줄씩 분석합니다.
기초 개념부터 설명하므로, Tokio/async를 처음 접하는 경우에도 이해할 수 있도록 작성했습니다.

---

## 전체 코드

```rust
async fn handle_socket_connection(
    stream: tokio::net::UnixStream,
    batch_service: Arc<B>,
    schedule_config: BatchScheduleConfig,
) -> anyhow::Result<()> {

    let (reader, writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);

    let (socket_tx, mut socket_rx) = mpsc::channel::<String>(256);
    tokio::spawn(async move {
        let mut w = writer;
        while let Some(msg) = socket_rx.recv().await {
            if w.write_all(msg.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let batch_items = schedule_config.get_enabled_schedules();

    loop {
        let mut menu = String::from("...");
        // ...메뉴 문자열 조립...
        socket_tx.send(menu).await?;

        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }

        let input = line.trim();
        if input == "0" || input.eq_ignore_ascii_case("q") {
            socket_tx.send("\nExiting.\n".to_string()).await?;
            break;
        }

        match input.parse::<usize>() {
            Ok(num) if num >= 1 && num <= batch_items.len() => {
                let schedule_item = batch_items[num - 1];
                let batch_name = schedule_item.batch_name();

                socket_tx.send(format!("\n[{}] Batch execution in progress...\n", batch_name)).await?;

                let (log_tx, mut log_rx) = mpsc::channel::<String>(256);

                let socket_tx_log = socket_tx.clone();
                tokio::spawn(async move {
                    while let Some(msg) = log_rx.recv().await {
                        let _ = socket_tx_log.send(format!("[LOG] {}\n", msg)).await;
                    }
                });

                let result = CLI_LOG_TX
                    .scope(Some(log_tx), batch_service.run_batch(schedule_item))
                    .await;

                match result {
                    Ok(()) => { socket_tx.send(format!("[{}] Complete.\n", batch_name)).await?; }
                    Err(e) => { socket_tx.send(format!("[{}] Failed: {}\n", batch_name, e)).await?; }
                }
            }
            _ => {
                socket_tx.send(format!("Please enter the correct number. (1-{})\n", batch_items.len())).await?;
            }
        }
    }

    Ok(())
}
```

---

## 기초 개념 먼저

### async / await란?

보통 함수는 호출하면 끝날 때까지 CPU를 점유합니다.
`async` 함수는 실행 중 다른 작업이 완료되길 기다릴 때 CPU를 반납하고, 완료되면 다시 돌아옵니다.

```
일반 함수: [──────────────────] (기다리는 동안에도 CPU 점유)
async 함수: [───]await[───]await[───] (await 중엔 CPU 반납, 다른 작업 실행 가능)
```

`.await`는 "이 작업이 완료될 때까지 기다려라. 단, 기다리는 동안 다른 일을 해도 된다"는 의미입니다.

task().await;

task 시작
↓
아직 준비 안됨
↓
현재 task suspend
↓
다른 task 실행
↓
task 준비되면 다시 실행

<--------------------->

### tokio::spawn이란?

`tokio::spawn(async { ... })`은 새로운 **비동기 태스크**를 생성합니다.
- OS 스레드와 다르게, 수천 개를 동시에 실행해도 메모리가 거의 안 늘어납니다
- Tokio 런타임이 스레드 풀에서 알아서 스케줄링합니다
- spawn한 태스크는 **백그라운드에서 독립적으로** 실행됩니다

```rust
tokio::spawn(async move {
    // 이 블록은 백그라운드에서 독립적으로 실행됨
    // 호출한 쪽은 기다리지 않고 바로 다음 줄로 넘어감
});
// ← 여기는 spawn과 동시에 실행됨
```

`move` 키워드는 블록 외부의 변수를 **소유권째로 가져와서** 태스크 안에서 사용한다는 의미입니다.

---

### mpsc 채널이란?

**mpsc = Multi-Producer Single-Consumer** (다수 송신자, 단일 수신자)

채널은 태스크 간 데이터를 안전하게 전달하는 "파이프"입니다.

```
[태스크 A] ──send("hello")──►┐
[태스크 B] ──send("world")──►├──► [채널 버퍼] ──recv()──► [태스크 C]
[태스크 D] ──send("!")──────►┘
```

- `mpsc::channel::<T>(N)` → `(Sender<T>, Receiver<T>)` 쌍을 반환
- `Sender`는 `.clone()`으로 복사해서 여러 태스크에서 동시에 보낼 수 있음
- `Receiver`는 복사 불가. 하나의 태스크만 받을 수 있음
- `N`은 버퍼 크기. 버퍼가 가득 차면 `send()`가 공간이 생길 때까지 대기함
- `Sender`가 모두 drop되면, `recv()`는 `None`을 반환하며 채널이 닫힘

**왜 쓰는가?** 여러 태스크가 같은 데이터 (소켓 writer)에 접근할 때, 직접 공유하면 동시 접근 문제가 생깁니다. 채널을 통해 한 태스크만 실제 쓰기를 담당하고, 나머지는 채널로 메시지를 보내는 방식으로 해결합니다.

---

### Arc란?

**Arc = Atomically Reference Counted** (원자적 참조 카운팅)

여러 태스크가 같은 데이터를 **읽기 전용으로 공유**할 때 사용합니다.

```rust
let shared = Arc::new(data);
let clone1 = Arc::clone(&shared); // 참조 카운트 +1
let clone2 = Arc::clone(&shared); // 참조 카운트 +1
// 모든 clone이 drop되면 참조 카운트 0 → 메모리 해제
```

`Rc`(단일 스레드용)와 달리 `Arc`는 멀티스레드/멀티태스크에서 안전합니다.

---

## 함수 시그니처

```rust
async fn handle_socket_connection(
    stream: tokio::net::UnixStream,  // 클라이언트와의 소켓 연결 (양방향 통신 가능)
    batch_service: Arc<B>,           // 배치 실행 서비스 (Arc로 공유됨)
    schedule_config: BatchScheduleConfig,  // 메뉴에 표시할 배치 목록 설정
) -> anyhow::Result<()>              // 성공 시 Ok(()), 실패 시 에러 반환
```

이 함수는 **CLI 클라이언트 1개의 연결 수명 전체**를 담당합니다.
클라이언트가 접속하면 이 함수가 호출되고, 클라이언트가 종료하면 이 함수도 끝납니다.

---

## 소켓 스트림 분리

```rust
let (reader, writer) = stream.into_split();
```

`UnixStream`은 읽기와 쓰기 모두 가능한 양방향 채널입니다.
하지만 Rust의 소유권 규칙상, 하나의 값을 동시에 두 곳에서 사용할 수 없습니다.

`into_split()`은 스트림을 두 개의 **독립적인 소유권**으로 분리합니다:

| 변수 | 타입 | 역할 |
|------|------|------|
| `reader` | `OwnedReadHalf` | 클라이언트가 보내는 데이터 읽기 전용 |
| `writer` | `OwnedWriteHalf` | 클라이언트에게 데이터 보내기 전용 |

"Owned"라는 이름이 중요합니다. 원본 스트림의 소유권을 반으로 쪼갠 것이기 때문에,
두 half를 서로 다른 태스크로 `move`해서 독립적으로 사용할 수 있습니다.

```
                  ┌─────────────────────────────────────┐
UnixStream ──────►│         into_split()                │
                  │                                     │
                  │  OwnedReadHalf  → 메인 루프에서 사용  │
                  │  OwnedWriteHalf → writer 태스크로 이동│
                  └─────────────────────────────────────┘
```

---

## BufReader로 감싸기

```rust
let mut reader = tokio::io::BufReader::new(reader);
```

`OwnedReadHalf`는 raw 읽기만 지원합니다. 줄 단위(`\n` 기준)로 읽으려면 `BufReader`가 필요합니다.

`BufReader`는 내부에 버퍼를 두고, 한 번에 많은 데이터를 읽어둔 뒤 `read_line()` 호출 시 버퍼에서 꺼내줍니다. 매번 소켓에서 1바이트씩 읽는 것보다 훨씬 효율적입니다.

---

## socket_tx 채널 생성 (핵심)

```rust
let (socket_tx, mut socket_rx) = mpsc::channel::<String>(256);
```

`socket_tx` (Sender)와 `socket_rx` (Receiver) 쌍을 생성합니다.

**왜 필요한가?**

이 함수 안에서 소켓에 쓰기를 하는 곳이 두 군데입니다:

1. **메인 루프**: 메뉴, "Complete." 등 상태 메시지
2. **로그 포워딩 태스크**: 배치 실행 중 `batch_log!()` 로 발생하는 실시간 로그

문제는 `OwnedWriteHalf`를 직접 두 곳에서 동시에 사용하면 컴파일 에러가 납니다.
Rust는 한 시점에 하나의 `&mut` 참조만 허용하기 때문입니다.

해결책: 소켓 쓰기는 writer 태스크 **딱 하나**만 담당하고,
나머지는 모두 `socket_tx`로 메시지를 보내면 그 태스크가 대신 씁니다.

```
메인 루프           socket_tx.send("메뉴")
로그 포워딩 태스크    socket_tx.clone().send("[LOG] ...")
                           │
                       [채널 버퍼]
                           │
                      socket_rx.recv()
                           │
                      [writer 태스크]
                      w.write_all() ──► 클라이언트 소켓
```

버퍼 크기 `256`은 최대 256개의 메시지를 버퍼에 쌓아둘 수 있다는 의미입니다.
배치 실행 중 로그가 빠르게 생성되더라도, writer 태스크가 처리하는 동안 버퍼에 보관됩니다.

---

## writer 태스크 생성

```rust
tokio::spawn(async move {
    let mut w = writer;
    while let Some(msg) = socket_rx.recv().await {
        if w.write_all(msg.as_bytes()).await.is_err() {
            break;
        }
    }
});
```

**라인별 분석:**

```rust
tokio::spawn(async move {
```
새 비동기 태스크를 백그라운드에서 시작합니다. `move`로 외부 변수(`writer`, `socket_rx`)의 소유권을 태스크 안으로 가져옵니다.

```rust
    let mut w = writer;
```
`writer`(OwnedWriteHalf)를 태스크 내부 변수 `w`에 바인딩합니다. `mut`는 `write_all()`이 `&mut self`를 요구하기 때문에 필요합니다.

```rust
    while let Some(msg) = socket_rx.recv().await {
```
채널에서 메시지를 하나씩 받습니다. `recv()`는 비동기라서 메시지가 올 때까지 CPU를 반납하며 기다립니다.
`Some(msg)`: 메시지가 도착함 → 루프 계속
`None`: 모든 `socket_tx`가 drop됨 → 루프 종료 (함수 종료 시 자동 발생)

```rust
        if w.write_all(msg.as_bytes()).await.is_err() {
            break;
        }
```
받은 문자열을 바이트로 변환해서 소켓에 씁니다. 클라이언트가 연결을 끊으면 에러가 발생하고 루프를 종료합니다.

**이 태스크의 수명:** `socket_rx`의 모든 `Sender`(socket_tx)가 drop될 때 자동으로 종료됩니다. `handle_socket_connection`이 반환(함수 끝)되면 `socket_tx`가 drop되므로, 이 태스크도 함께 정리됩니다.

---

## 메인 루프

```rust
let batch_items = schedule_config.get_enabled_schedules();

loop {
```

`batch_items`는 활성화된 배치 목록으로, 메뉴 생성에 사용됩니다.
`loop`는 클라이언트가 종료(0/q 입력 또는 연결 끊김)할 때까지 반복합니다.

---

### 메뉴 전송

```rust
    let mut menu = String::from("\n==============================\nSelect a batch to execute:\n");
    for (i, item) in batch_items.iter().enumerate() {
        menu.push_str(&format!("  {}. {}\n", i + 1, item.batch_name()));
    }
    menu.push_str("  0. Exit\n");
    menu.push_str("==============================\n");
    menu.push_str("Input:\n");

    socket_tx.send(menu).await?;
```

메뉴 문자열을 조립해서 채널로 보냅니다. writer 태스크가 받아서 소켓에 씁니다.

마지막 줄 `"Input:\n"`이 중요합니다. 클라이언트(`cli_client_controller.rs`)의 `read_until_prompt()`는 `"Input:"` 문자열을 받으면 읽기를 멈추고 사용자 입력을 기다립니다. 반드시 `\n`이 있어야 `read_line()`이 복귀합니다.

`await?`: `.await`로 전송 완료를 기다리고, 에러 발생 시 `?`로 함수 전체를 에러와 함께 반환합니다.

---

### 클라이언트 입력 수신

```rust
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;

    if n == 0 {
        break; // Client Disconnected
    }
```

`read_line()`은 클라이언트가 `\n`을 보낼 때까지 블로킹합니다. (async이므로 CPU는 반납)
반환값 `n`은 읽은 바이트 수입니다. `0`이면 클라이언트가 연결을 끊은 것입니다 (EOF).

```rust
    let input = line.trim();
```

`"3\n"` → `"3"` 으로 앞뒤 공백/줄바꿈을 제거합니다.

---

### 종료 명령 처리

```rust
    if input == "0" || input.eq_ignore_ascii_case("q") {
        socket_tx.send("\nExiting.\n".to_string()).await?;
        break;
    }
```

`"0"` 또는 `"q"`/`"Q"` 입력 시 종료 메시지를 보내고 루프를 탈출합니다.
루프가 끝나면 함수가 반환되고, `socket_tx`가 drop → writer 태스크도 종료됩니다.

---

### 배치 번호 파싱

```rust
    match input.parse::<usize>() {
        Ok(num) if num >= 1 && num <= batch_items.len() => {
```

`"3"` → `3usize`로 변환 시도합니다. 성공하고 유효한 범위(1 이상, 목록 크기 이하)면 배치를 실행합니다.

`if num >= 1 && num <= batch_items.len()` 부분은 **match guard**입니다. 패턴 매칭 후 추가 조건을 검사합니다. 조건을 만족하지 않으면 `_` 분기로 떨어집니다.

---

## 배치 실행 + 실시간 로그 스트리밍 (핵심)

### 시작 메시지 전송

```rust
            socket_tx
                .send(format!("\n[{}] Batch execution in progress...\n", batch_name))
                .await?;
```

배치 실행 전 CLI 화면에 시작 메시지를 표시합니다.

---

### 로그 채널 생성

```rust
            let (log_tx, mut log_rx) = mpsc::channel::<String>(256);
```

배치 실행 전용 로그 채널을 새로 만듭니다.

이 채널은 **이번 배치 실행 1회에만** 사용됩니다. 배치가 끝나면 `log_tx`가 drop되어 채널도 닫힙니다.

여러 CLI 클라이언트가 동시에 접속해도, 각자 독립적인 `log_tx`를 가지므로 **다른 배치의 로그가 섞이지 않습니다**.

---

### 로그 포워딩 태스크

```rust
            let socket_tx_log = socket_tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = log_rx.recv().await {
                    let _ = socket_tx_log
                        .send(format!("[LOG] {}\n", msg))
                        .await;
                }
            });
```

**라인별 분석:**

```rust
            let socket_tx_log = socket_tx.clone();
```
`socket_tx`는 `Clone` 가능한 `Sender`입니다. 복사해서 포워딩 태스크에 넘깁니다. 원본 `socket_tx`는 메인 루프에서 계속 사용합니다.

```rust
            tokio::spawn(async move {
```
새 태스크를 백그라운드로 시작합니다. `log_rx`와 `socket_tx_log` 소유권을 태스크 안으로 이동합니다.

```rust
                while let Some(msg) = log_rx.recv().await {
```
`batch_log!(info, "...")` 매크로가 `log_tx`로 메시지를 보낼 때마다 깨어납니다.

```rust
                    let _ = socket_tx_log.send(format!("[LOG] {}\n", msg)).await;
```
`[LOG]` 접두사를 붙여서 socket_tx를 통해 실제 소켓에 전달합니다.
`let _ =` 는 에러를 무시한다는 의미입니다. 클라이언트가 중간에 끊겨도 패닉하지 않습니다.

**이 태스크의 수명:** `log_tx`가 drop될 때 `log_rx.recv()`가 `None` → 루프 종료 → 태스크 종료.

---

### task-local로 배치 실행

```rust
            let result = CLI_LOG_TX
                .scope(Some(log_tx), batch_service.run_batch(schedule_item))
                .await;
```

이 줄이 이 함수에서 가장 복잡한 부분입니다. 분해해서 이해합니다.

**`CLI_LOG_TX`란?**

`utils_module/cli_log.rs`에 정의된 task-local 변수입니다:

```rust
tokio::task_local! {
    pub static CLI_LOG_TX: Option<mpsc::Sender<String>>;
}
```

task-local은 특정 async 태스크 안에서만 유효한 저장소입니다.
스레드 로컬(`thread_local!`)과 유사하지만, OS 스레드가 아닌 Tokio 태스크 단위입니다.

**`.scope(value, future)`란?**

```
CLI_LOG_TX.scope(Some(log_tx), future) 실행 구조:
  ┌─────────────────────────────────────┐
  │ CLI_LOG_TX = Some(log_tx) 로 설정   │
  │                                     │
  │   future (= run_batch()) 실행       │
  │         │                           │
  │         └─ .await로 완료 대기        │
  │                                     │
  │ scope 종료 → log_tx drop           │
  └─────────────────────────────────────┘
```

`scope` 안에서 실행되는 모든 코드(`.await`로 연결된 함수 체인 전체)에서 `CLI_LOG_TX.try_with()`로 `log_tx`에 접근할 수 있습니다.

**`batch_log!(info, "...")` 매크로 내부:**

```rust
macro_rules! batch_log {
    (info, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        log::info!("{}", msg);           // 파일 + 서비스 stdout (항상 실행)
        let _ = CLI_LOG_TX.try_with(|opt| {
            if let Some(tx) = opt {
                let _ = tx.try_send(msg); // log_tx로 전송 (scope 안에서만 작동)
            }
        });
    }};
}
```

- `log::info!()`: flexi_logger를 통해 파일과 서비스 터미널에 기록 (항상 실행)
- `CLI_LOG_TX.try_with()`: task-local에 값이 설정된 경우(= CLI scope 안)에만 `log_tx`로 전송

**크론 스케줄러에서 실행되는 배치:**

```
tokio::spawn(async move {
    // ← 새 태스크 생성. CLI_LOG_TX가 설정되지 않음
    Self::process_batch(...).await
    // batch_log! 호출 → log::info!() 는 실행되지만
    //                   CLI_LOG_TX.try_with() 는 AccessError → log_tx 전송 안 함
})
```

같은 `batch_log!()` 코드지만, task-local 설정 여부에 따라 CLI 전달 여부가 결정됩니다. 의도된 동작으로, 내 CLI 세션이 아닌 다른 배치의 로그는 받지 않습니다.

---

### 배치 완료 처리

```rust
            match result {
                Ok(()) => {
                    socket_tx.send(format!("[{}] Complete.\n", batch_name)).await?;
                }
                Err(e) => {
                    socket_tx.send(format!("[{}] Failed: {}\n", batch_name, e)).await?;
                }
            }
```

`scope`가 끝난 시점(= `run_batch()` 완료 후)에 최종 결과를 클라이언트에 전송합니다.

`scope` 종료 시 `log_tx`가 drop → 포워딩 태스크의 `log_rx.recv()`가 `None` 반환 → 포워딩 태스크 자동 종료.

즉, "Complete." 메시지가 전송되는 시점에는 로그 포워딩 태스크가 이미 종료 중입니다. (채널 버퍼에 남은 로그는 모두 flush된 후 종료)

---

### 잘못된 입력 처리

```rust
        _ => {
            socket_tx
                .send(format!("Please enter the correct number. (1-{})\n", batch_items.len()))
                .await?;
        }
```

숫자가 아니거나 범위를 벗어난 경우 안내 메시지를 전송합니다. 루프는 계속되어 메뉴를 다시 표시합니다.

---

## 전체 생명주기 요약

```
handle_socket_connection() 시작
    │
    ├─ stream.into_split() → reader, writer
    │
    ├─ mpsc::channel() → socket_tx, socket_rx
    │
    ├─ tokio::spawn(writer 태스크) → socket_rx 소유권 이동, 백그라운드 대기
    │
    └─ loop {
            socket_tx.send(메뉴)               → writer 태스크가 클라이언트에 전송
            reader.read_line()                 → 클라이언트 입력 대기

            [배치 선택 시]
            mpsc::channel() → log_tx, log_rx  (배치 전용 채널)
            tokio::spawn(로그 포워딩 태스크)    → log_rx 소유, 백그라운드 대기
            CLI_LOG_TX.scope(log_tx, run_batch())
                → process_batch()
                    → batch_log!() → log::info!() + log_tx.try_send()
                                        ↓
                                   포워딩 태스크 → socket_tx → writer 태스크 → 소켓
            scope 종료 → log_tx drop → 포워딩 태스크 종료
            socket_tx.send("Complete.") 또는 "Failed:"
       }

    loop 종료 (0/q 입력 또는 EOF)
    함수 반환 → socket_tx drop → writer 태스크 종료
```

---

## 태스크 구조 전체 그림

```
handle_socket_connection (메인 루프 태스크)
    │
    ├─── [writer 태스크] (백그라운드, 함수 전체 수명)
    │         socket_rx.recv() → w.write_all()
    │
    └─── [로그 포워딩 태스크] (배치 실행 중에만, 1회성)
              log_rx.recv() → socket_tx_log.send("[LOG] ...")
                                    │
                                    ▼
                             [writer 태스크]의 socket_rx
                                    │
                                    ▼
                             클라이언트 소켓
```

모든 소켓 쓰기는 결국 writer 태스크 하나를 통해 직렬화됩니다. 동시성 문제가 발생하지 않습니다.
