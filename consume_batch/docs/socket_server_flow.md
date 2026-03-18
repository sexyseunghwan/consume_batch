# main_task → start_socket_server → handle_socket_connection 흐름 완전 분석

`MainController::main_task`부터 `handle_socket_connection`까지 전체 실행 흐름을 처음 접하는 사람도 이해할 수 있도록 설명합니다.

---

## 목차

1. [전제: 이 프로젝트의 두 가지 모드](#1-전제-이-프로젝트의-두-가지-모드)
2. [핵심 배경 개념](#2-핵심-배경-개념)
3. [MainController::main_task](#3-maincontrollermain_task)
4. [CliServiceImpl::start_socket_server](#4-cliserviceimplstart_socket_server)
5. [handle_socket_connection — 연결 1개의 전체 수명](#5-handle_socket_connection--연결-1개의-전체-수명)
6. [실시간 로그 스트리밍 원리](#6-실시간-로그-스트리밍-원리)
7. [태스크 트리 전체 그림](#7-태스크-트리-전체-그림)
8. [데이터 흐름 요약](#8-데이터-흐름-요약)

---

## 1. 전제: 이 프로젝트의 두 가지 모드

같은 바이너리(`consume_batch_v1`)가 실행 인자에 따라 두 가지로 동작합니다.

```
./consume_batch_v1        → 서비스 모드
                             - 배치 스케줄러 (크론 잡) 실행
                             - Unix 소켓 서버 열어서 CLI 연결 대기

./consume_batch_v1 --cli  → CLI 클라이언트 모드
                             - Unix 소켓에 접속해서 메뉴를 받고 배치를 수동 실행
```

이 문서는 **서비스 모드**에서 소켓 서버가 어떻게 동작하는지를 다룹니다.

---

## 2. 핵심 배경 개념

본문을 읽기 전에 아래 개념들을 먼저 이해하면 훨씬 쉽습니다.

---

### async / await

보통 함수는 호출하면 완료될 때까지 CPU를 점유합니다.
`async` 함수는 "기다려야 하는 순간"에 CPU를 반납하고, 다른 작업이 실행될 수 있게 양보합니다.

```
일반 함수:  [──────────────────────────] (기다리는 중에도 CPU 점유)

async 함수: [──]await[──]await[──await[──]
                  ↑          ↑
             CPU 반납    CPU 반납
             (다른 일    (다른 일
              처리 가능)  처리 가능)
```

`.await`는 "이 작업이 끝날 때까지 기다려라. 단 기다리는 동안은 다른 일을 해도 된다"는 의미입니다.

---

### Tokio 런타임과 tokio::spawn

Tokio는 async Rust의 실행 환경(런타임)입니다. OS 스레드 여러 개를 관리하며, 그 위에서 수천 개의 **비동기 태스크**를 스케줄링합니다.

`tokio::spawn(async { ... })`은 새로운 비동기 태스크를 Tokio 런타임에 등록합니다.

```rust
tokio::spawn(async move {
    // 이 블록은 백그라운드에서 독립적으로 실행됨
    // spawn한 태스크는 기다리지 않고 바로 다음 줄로 넘어감
});
// ← 이 줄은 위 spawn과 거의 동시에 실행됨
```

- OS 스레드와 달리 매우 가볍습니다 (수천 개도 OK)
- `move` 키워드: 외부 변수의 소유권을 태스크 안으로 이동

---

### mpsc 채널

**mpsc = Multi-Producer Single-Consumer** (다수 송신자, 단일 수신자)

서로 다른 태스크 사이에서 데이터를 안전하게 전달하는 "파이프"입니다.

```
[태스크 A] ──send("hello")──►┐
[태스크 B] ──send("world")──►├──► [채널 버퍼 N개] ──recv()──► [태스크 C]
[태스크 D] ──send("!")──────►┘
```

```rust
let (tx, mut rx) = mpsc::channel::<String>(256);
// tx: Sender   - .clone()으로 복사해서 여러 태스크가 보낼 수 있음
// rx: Receiver - 복사 불가, 하나의 태스크만 받을 수 있음
// 256: 버퍼 크기 (가득 차면 send()가 공간이 생길 때까지 대기)

// 모든 tx(Sender)가 drop되면 → rx.recv()가 None 반환 → 채널 닫힘
```

---

### Arc (Atomically Reference Counted)

여러 태스크가 같은 데이터를 공유할 때 사용합니다.

```rust
let shared = Arc::new(data);
let clone1 = Arc::clone(&shared); // 참조 카운트 +1, data는 복사되지 않음
let clone2 = Arc::clone(&shared); // 참조 카운트 +1

// 모든 clone이 drop되면 → 참조 카운트 0 → 메모리 해제
```

`Arc::clone`은 데이터를 복사하는 게 아니라 "이 데이터를 가리키는 포인터를 하나 더 만드는 것"입니다.

---

### Unix 도메인 소켓

일반 TCP 소켓은 IP:포트로 연결하지만, Unix 소켓은 **파일시스템 경로**로 연결합니다.

```
TCP:  192.168.1.1:8080
Unix: ./socket/consume_batch.sock  (파일 경로)
```

같은 머신 내 프로세스 간 통신에 사용됩니다. TCP보다 빠르고 외부에서 접근할 수 없어 안전합니다.

---

### task-local 변수

`thread_local!`은 OS 스레드 단위로 값을 저장합니다.
`tokio::task_local!`은 **비동기 태스크 단위**로 값을 저장합니다.

```rust
tokio::task_local! {
    static MY_VAR: SomeType;
}

// scope(value, future): future가 실행되는 동안만 MY_VAR = value 로 설정
MY_VAR.scope(value, async {
    // 이 블록 안에서만 MY_VAR.try_with()로 value에 접근 가능
    MY_VAR.try_with(|v| { /* v는 value */ });
}).await;
// scope 종료 후 MY_VAR은 설정 해제
```

핵심: `.await`로 연결된 함수 체인 전체(= 같은 태스크 내)에서 접근 가능합니다.

---

## 3. MainController::main_task

```rust
pub async fn main_task(&self) -> anyhow::Result<()> {
    // [1] CLI 소켓 서버를 백그라운드 태스크로 실행
    {
        let cli_service: Arc<CS> = Arc::clone(&self.cli_service);

        tokio::spawn(async move {
            if let Err(e) = cli_service.start_socket_server().await {
                error!("[MainController::main_task] CLI socket server error: {:#}", e);
            }
        });
    }

    // [2] 배치 스케줄러를 포그라운드에서 실행 (Ctrl+C까지 블로킹)
    match self.batch_service.main_batch_task().await {
        Ok(_) => (),
        Err(e) => { error!("[MainController::main_task] {:#}", e); }
    }

    Ok(())
}
```

### 왜 CLI는 spawn하고 batch는 직접 await하는가?

```
main_task()
    │
    ├─ tokio::spawn ──────────────────────────────────── [백그라운드]
    │       └─ cli_service.start_socket_server()
    │               └─ loop { listener.accept() ... }
    │                  (무한루프로 연결을 계속 받음)
    │
    └─ batch_service.main_batch_task().await ────────── [포그라운드]
            └─ scheduler 시작
            └─ Ctrl+C 신호 대기
            └─ Ctrl+C 수신 → scheduler 종료 → 함수 반환
```

- `start_socket_server`는 `loop`를 돌며 **영원히 실행**됩니다. `await`로 직접 부르면 배치 스케줄러가 절대 실행되지 않습니다.
- 따라서 `tokio::spawn`으로 백그라운드에 띄운 뒤, 포그라운드에서 배치 스케줄러를 실행합니다.
- `Arc::clone(&self.cli_service)`: `spawn` 클로저는 `'static` (정적 수명)이어야 하기 때문에, `&self`를 직접 넘길 수 없습니다. `Arc`로 감싸서 소유권을 태스크 안으로 이동시킵니다.

---

## 4. CliServiceImpl::start_socket_server

```rust
async fn start_socket_server(&self) -> anyhow::Result<()> {
    let socket_path: &str = AppConfig::global().socket_path();

    // [1] 이전 소켓 파일 제거 (비정상 종료 후 남은 파일 처리)
    std::fs::remove_file(socket_path)
        .inspect_err(|e| {
            error!("[CliServiceImpl::start_socket_server] {:#}", e);
        })?;

    // [2] 소켓 파일 생성 및 바인딩
    let listener: UnixListener = UnixListener::bind(socket_path)
        .inspect_err(|e| {
            error!("[CliServiceImpl::start_socket_server] Failed to bind socket: {:#}", e);
        })?;

    info!("CLI socket server listening on {}", socket_path);

    // [3] 연결 수락 루프
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .inspect_err(|e| {
                error!("[CliServiceImpl::start_socket_server] Failed to accept connection: {:#}", e);
            })?;

        let batch: Arc<B> = Arc::clone(&self.batch_service);
        let config: BatchScheduleConfig = self.schedule_config.clone();

        // [4] 연결 1개당 독립 태스크 생성
        tokio::spawn(async move {
            if let Err(e) = Self::handle_socket_connection(stream, batch, config).await {
                error!(
                    "[CliServiceImpl::handle_socket_connection] Connection error: {:#}",
                    e
                );
            }
        });
    }
}
```

### 소켓 파일을 미리 지우는 이유

Unix 소켓은 파일시스템에 실제 파일(`consume_batch.sock`)을 만듭니다. 서비스가 정상 종료하면 이 파일을 삭제하지만, **비정상 종료(kill, crash)**하면 파일이 남아 있습니다. 다음 실행 시 `UnixListener::bind`가 "이미 존재하는 파일"이라며 에러를 냅니다. 그래서 바인딩 전에 먼저 파일을 삭제합니다.

### UnixListener::bind vs accept

```
bind(path)   → 소켓 파일 생성, "여기로 연결하면 받겠다" 선언
accept()     → 실제 클라이언트가 연결할 때까지 대기
              → 연결이 오면 그 연결을 나타내는 UnixStream 반환
              → await이므로 기다리는 동안 CPU 반납
```

### 연결마다 spawn하는 이유

`accept()` 루프는 계속 다음 연결을 받을 준비를 해야 합니다. `handle_socket_connection`을 직접 `await`하면 그 연결이 끝날 때까지 다음 클라이언트를 받지 못합니다.

```
// 잘못된 방식 (직렬):
loop {
    let (stream, _) = listener.accept().await?;
    handle_socket_connection(stream, ...).await?;  // 이 연결이 끝날 때까지 다음 accept 불가
}

// 올바른 방식 (병렬):
loop {
    let (stream, _) = listener.accept().await?;
    tokio::spawn(handle_socket_connection(stream, ...));  // 백그라운드, 즉시 다음 accept 가능
}
```

`Arc::clone(&self.batch_service)`: `batch_service`를 spawn 클로저 안으로 이동시키기 위해 clone합니다. 
실제 데이터는 복사되지 않고, 포인터만 하나 더 만들어집니다.

---

## 5. handle_socket_connection — 연결 1개의 전체 수명

이 함수는 CLI 클라이언트 **1개의 연결 전체**를 담당합니다.
클라이언트가 접속하면 호출되고, 클라이언트가 종료(또는 연결 끊김)하면 반환됩니다.

### 5-1. 스트림 분리

```rust
let (reader, writer) = stream.into_split();
let mut reader = tokio::io::BufReader::new(reader);
```

`UnixStream`은 읽기/쓰기 모두 가능한 양방향 채널입니다. 하지만 Rust에서는 하나의 값을 동시에 두 곳에서 `&mut`로 참조할 수 없습니다.

`into_split()`은 스트림을 두 개의 독립 소유권으로 분리합니다:

| 변수 | 타입 | 역할 |
|------|------|------|
| `reader` | `OwnedReadHalf`  | 클라이언트에서 오는 데이터 읽기 전용 |
| `writer` | `OwnedWriteHalf` | 클라이언트에게 데이터 쓰기 전용 |

"Owned"는 원본의 소유권을 반으로 나눈 것입니다. 각각을 서로 다른 태스크로 `move`해서 독립적으로 사용할 수 있습니다.

`BufReader`로 감싸는 이유: `OwnedReadHalf`는 raw 바이트 읽기만 지원합니다. 줄 단위(`\n` 기준)로 읽으려면 `BufReader`가 필요합니다. `BufReader`는 내부 버퍼에 여러 바이트를 미리 읽어두므로, 매번 소켓에서 1바이트씩 읽는 것보다 훨씬 효율적입니다.

---

### 5-2. socket_tx 채널과 writer 태스크 (핵심 구조)

```rust
let (socket_tx, mut socket_rx) = mpsc::channel::<String>(256);

tokio::spawn(async move {
    let mut w = writer;
    while let Some(msg) = socket_rx.recv().await {
        if w.write_all(msg.as_bytes()).await.is_err() {
            break;
        }
    }
});
```

**왜 이 구조가 필요한가?**

이 함수 안에서 소켓에 쓰기를 하는 곳이 **두 군데**입니다:

1. **메인 루프**: 메뉴 화면, "Complete.", "Failed:" 등 상태 메시지
2. **로그 포워딩 태스크**: 배치 실행 중 `batch_log!()`로 발생하는 실시간 로그

`OwnedWriteHalf`는 `&mut` 참조만 허용하므로, 두 태스크가 동시에 직접 쓸 수 없습니다. Rust 컴파일러가 이를 거부합니다.

**해결책**: 소켓 쓰기는 `writer 태스크` 하나만 담당합니다. 나머지는 `socket_tx`로 메시지를 보내기만 하면, writer 태스크가 받아서 씁니다.

```
메인 루프           ──socket_tx.send("메뉴")────────►┐
로그 포워딩 태스크    ──socket_tx.clone().send("LOG")►├──► [채널 버퍼 256] ──socket_rx.recv()──► writer 태스크 ──► 클라이언트 소켓
                                                   ┘
```

**writer 태스크 수명**: `socket_tx`(Sender)가 모두 drop되면 `socket_rx.recv()`가 `None`을 반환 → 루프 종료 → 태스크 종료. `handle_socket_connection`이 반환될 때 `socket_tx`가 drop되므로, writer 태스크도 자동으로 정리됩니다.

**클라이언트 연결 끊김 감지**: `w.write_all().is_err()`가 `true`이면 클라이언트가 연결을 끊은 것입니다. 루프를 `break`합니다.

핵심 문제부터 시작할게요.

"왜 채널이 필요한가?" — 문제 상황부터
writer(소켓에 쓰는 개체)를 직접 쓰려면 &mut writer가 필요합니다. Rust에서 &mut는 한 시점에 딱 하나만 존재할 수 있습니다.

그런데 이 함수에서 소켓에 쓰기를 하고 싶은 곳이 두 군데 생깁니다:


나중에 생기는 두 곳:

[메인 루프]          → 메뉴 화면, "Complete.", "Failed:" 등
[로그 포워딩 태스크] → 배치 실행 중 "[LOG] ..." 실시간 로그
만약 채널 없이 writer를 직접 두 곳에서 쓰려고 하면:


// 이렇게 하고 싶지만 → 컴파일 에러
tokio::spawn(async move {
    writer.write_all(log_msg).await;  // writer 소유권을 이 태스크가 가져감
});

writer.write_all(menu).await;  // ← 이미 위 태스크가 가져갔으므로 사용 불가
writer의 소유권이 하나인데 두 군데서 쓸 수 없습니다.

해결책: "쓰기 담당자를 하나로 고정"

직접 쓰기 (불가능):
메인 루프          ──write──► writer(소켓)   ← 두 개가 동시에 접근 불가
로그 포워딩 태스크  ──write──► writer(소켓)

채널을 통한 간접 쓰기 (가능):
메인 루프          ──send──► [채널]
로그 포워딩 태스크   ──send──► [채널] ──recv──► writer 태스크 ──write──► writer(소켓)
writer는 writer 태스크 하나만 독점합니다. 나머지는 채널로 메시지를 "주문"하기만 하면, writer 태스크가 받아서 소켓에 씁니다.

코드를 순서대로 다시 보면

// ① 채널 생성: tx(주문표), rx(주문 받는 쪽)
let (socket_tx, mut socket_rx) = mpsc::channel::<String>(256);

// ② writer 태스크 생성: writer 소유권을 이 태스크 안으로 이동
tokio::spawn(async move {
    let mut w = writer;  // writer는 이제 이 태스크 소유
    while let Some(msg) = socket_rx.recv().await {  // 주문이 올 때마다
        if w.write_all(msg.as_bytes()).await.is_err() {
            break;
        }
    }
});

// ③ 이제 메인 루프는 socket_tx로 "주문"만 하면 됨
socket_tx.send(menu).await?;       // 메뉴 보내줘
socket_tx.send("Complete.").await?; // 완료 메시지 보내줘
socket_tx는 clone()이 되기 때문에, 로그 포워딩 태스크도 복사본을 받아서 같은 채널에 주문할 수 있습니다:


let socket_tx_log = socket_tx.clone();  // 복사본 (실제 채널은 하나)
tokio::spawn(async move {
    while let Some(msg) = log_rx.recv().await {
        let _ = socket_tx_log.send(format!("[LOG] {}\n", msg)).await;  // 같은 채널에 주문
    }
});
최종 그림

[메인 루프]
  socket_tx.send("메뉴")      ──►┐
  socket_tx.send("Complete.") ──►│
                                 │
[로그 포워딩 태스크]               ├──► [채널 버퍼, 최대 256개]
  socket_tx_log.send("[LOG]...") ►│                │
                                  │         socket_rx.recv()
                                  │                │
                                  │         [writer 태스크]
                                  │         w.write_all()
                                  │                │
                                              클라이언트 소켓
버퍼 256의 의미: 배치 실행 중 로그가 빠르게 쏟아져도, writer 태스크가 하나씩 처리하는 동안 채널 버퍼에 최대 256개까지 쌓아둘 수 있습니다. 
버퍼가 가득 차면 send()가 공간이 생길 때까지 잠시 대기합니다.

한 줄 요약: writer를 두 곳에서 동시에 쓸 수 없으니, 채널을 "우편함"으로 만들고 전담 직원(writer 태스크) 하나가 우편함을 비워가며 소켓에 쓰는 구조입니다.


---

### 5-3. 메인 루프 (메뉴 → 입력 → 분기)

```rust
let batch_items = schedule_config.get_enabled_schedules();

loop {
    // [1] 메뉴 조립 및 전송
    let mut menu = String::from("\n==============================\nSelect a batch:\n");
    for (i, item) in batch_items.iter().enumerate() {
        menu.push_str(&format!("  {}. {}\n", i + 1, item.batch_name()));
    }
    menu.push_str("  0. Exit\n==============================\nInput:\n");
    socket_tx.send(menu).await?;

    // [2] 클라이언트 입력 대기
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 { break; }  // EOF = 클라이언트 연결 끊김

    let input = line.trim();

    // [3] 종료 명령
    if input == "0" || input.eq_ignore_ascii_case("q") {
        socket_tx.send("\nExiting.\n".to_string()).await?;
        break;
    }

    // [4] 배치 번호 파싱 및 실행
    match input.parse::<usize>() {
        Ok(num) if num >= 1 && num <= batch_items.len() => {
            // → 배치 실행 (5-4 참고)
        }
        _ => {
            socket_tx.send(format!("Please enter 1-{}\n", batch_items.len())).await?;
        }
    }
}
```

**`"Input:\n"`의 역할**: 클라이언트(`cli_client_controller.rs`)의 `read_until_prompt()`는 `"Input:"` 문자열을 받으면 읽기를 멈추고 사용자 입력을 받습니다. 반드시 `\n`이 있어야 `read_line()`이 복귀합니다. `\n` 없이 `"Input:"`만 보내면 클라이언트의 `read_line()`이 영원히 대기하는 **데드락**이 발생합니다.

**`await?` 패턴**: `.await`로 비동기 완료를 기다리고, `?`로 에러 발생 시 함수를 즉시 반환합니다. 소켓이 끊기거나 채널이 닫히면 에러를 반환해 자연스럽게 함수가 종료됩니다.

**`read_line()` 반환값 `n == 0`**: TCP/Unix 소켓에서 `n == 0`은 **EOF**를 의미합니다. 클라이언트가 연결을 강제로 끊으면(Ctrl+C, 네트워크 오류 등) 이 조건이 참이 됩니다.

**match guard** (`Ok(num) if num >= 1 && num <= batch_items.len()`): 패턴 매칭 후 추가 조건을 검사합니다. 조건 불만족 시 `_` 분기로 떨어집니다.

---

### 5-4. 배치 선택 시 실행 흐름

```rust
let schedule_item = batch_items[num - 1];
let batch_name = schedule_item.batch_name();

// [1] 시작 메시지 전송
socket_tx.send(format!("\n[{}] Batch execution in progress...\n", batch_name)).await?;

// [2] 배치 전용 로그 채널 생성
let (log_tx, mut log_rx) = mpsc::channel::<String>(256);

// [3] 로그 포워딩 태스크 생성
let socket_tx_log = socket_tx.clone();
tokio::spawn(async move {
    while let Some(msg) = log_rx.recv().await {
        let _ = socket_tx_log.send(format!("[LOG] {}\n", msg)).await;
    }
});

// [4] task-local에 log_tx를 설정하고 배치 실행
let result = CLI_LOG_TX
    .scope(Some(log_tx), batch_service.run_batch(schedule_item))
    .await;

// [5] 결과 전송
match result {
    Ok(()) => { socket_tx.send(format!("[{}] Complete.\n", batch_name)).await?; }
    Err(e) => { socket_tx.send(format!("[{}] Failed: {}\n", batch_name, e)).await?; }
}
```

각 단계를 상세히 설명합니다.

---

## 6. 실시간 로그 스트리밍 원리

이 프로젝트의 가장 독창적인 부분입니다. 배치 실행 중 발생하는 로그가 CLI 클라이언트 화면에 실시간으로 표시됩니다.

### 6-1. CLI_LOG_TX task-local 변수

`utils_module/cli_log.rs`:

```rust
tokio::task_local! {
    pub static CLI_LOG_TX: Option<mpsc::Sender<String>>;
}
```

이 변수는 특정 태스크 안에서만 유효한 저장소입니다. 값이 설정되지 않은 태스크에서 `try_with()`를 호출하면 `AccessError`를 반환합니다. 에러가 아니라 "값이 없다"는 신호이므로, `if let` 등으로 안전하게 처리합니다.

### 6-2. batch_log! 매크로 내부

`batch_service_impl.rs` 상단에 정의된 매크로:

```rust
macro_rules! batch_log {
    (info, $($arg:tt)*) => {{
        let msg = format!($($arg)*);

        // 항상 실행: 파일 + 서비스 터미널 stdout에 기록
        log::info!("{}", msg);

        // CLI scope 안에서만 실행: log_tx로 메시지 전송
        let _ = CLI_LOG_TX.try_with(|opt| {
            if let Some(tx) = opt {
                let _ = tx.try_send(msg);  // non-blocking send
            }
        });
    }};
}
```

`try_send`: 채널 버퍼가 가득 찼을 때 블로킹하지 않고 에러를 반환합니다. 로그 하나가 느린 소켓 때문에 배치 처리 전체를 막지 않도록 하기 위해 사용합니다.

### 6-3. scope의 역할

```
CLI_LOG_TX.scope(Some(log_tx), run_batch(schedule_item))
```

```
scope 진입
    │
    ├─ CLI_LOG_TX = Some(log_tx) 로 설정
    │
    └─ run_batch(schedule_item).await
            │
            └─ process_batch() → process_spent_detail_full() → ...
                    │
                    ├─ batch_log!(info, "Starting...") 호출
                    │       ├─ log::info!()           → 파일/서비스 터미널 기록
                    │       └─ CLI_LOG_TX.try_with()  → log_tx.try_send("Starting...")
                    │                                          │
                    │                                   log_rx.recv()
                    │                                          │
                    │                              로그 포워딩 태스크
                    │                                          │
                    │                              socket_tx.send("[LOG] Starting...\n")
                    │                                          │
                    │                                  writer 태스크
                    │                                          │
                    │                               클라이언트 소켓 ──► CLI 화면 출력
                    │
                    └─ batch_log!(info, "Done") 호출 (반복)

scope 종료
    │
    └─ log_tx drop
           │
     log_rx.recv() → None
           │
     로그 포워딩 태스크 종료
```

**크론 스케줄러 배치와의 차이**:

```
tokio::spawn(async move {
    // ← 새 태스크 생성. CLI_LOG_TX 설정 없음
    process_batch(...).await
    // batch_log!() 호출 시:
    //   log::info!() → 실행됨 (파일/서비스 터미널)
    //   CLI_LOG_TX.try_with() → AccessError → log_tx 전송 안 함
})
```

같은 `batch_log!()` 코드지만, task-local이 설정된 태스크(CLI 실행)에서만 CLI로 전달됩니다. **다른 스케줄러 배치의 로그가 내 CLI 화면에 섞이지 않는** 이유입니다.

### 6-4. scope 종료와 로그 포워딩 태스크 정리

```
scope 종료
    → log_tx drop
    → 로그 포워딩 태스크: log_rx.recv() = None → while 루프 종료 → 태스크 종료
    → 채널에 남은 버퍼는 모두 처리된 후 종료 (graceful)

이후:
    → match result { Ok/Err } → "Complete." 또는 "Failed:" 전송
```

"Complete." 메시지가 전송되는 시점에는 로그 포워딩 태스크가 이미 정리 중(남은 로그 flush 완료)입니다.

---

## 7. 태스크 트리 전체 그림

서비스 기동부터 배치 실행 중까지 활성화되는 모든 태스크입니다.

```
Tokio 런타임
    │
    ├─ [main task]
    │       └─ MainController::main_task()
    │               │
    │               ├─ tokio::spawn ──────────────────────────────── [CLI 서버 태스크]
    │               │                   start_socket_server()
    │               │                       │
    │               │                       └─ loop { accept().await }
    │               │                               │
    │               │                          (클라이언트 접속 시)
    │               │                               │
    │               │                    tokio::spawn ──────────── [연결 핸들러 태스크 #1]
    │               │                                   handle_socket_connection()
    │               │                                       │
    │               │                           tokio::spawn ──── [writer 태스크]
    │               │                                               socket_rx → write_all()
    │               │                                       │
    │               │                           (배치 실행 중)
    │               │                                       │
    │               │                           tokio::spawn ──── [로그 포워딩 태스크]
    │               │                                               log_rx → socket_tx
    │               │
    │               └─ batch_service.main_batch_task().await ─── [배치 스케줄러, 포그라운드]
    │                       │
    │                       ├─ tokio::spawn ──── [크론 잡 태스크 #1] process_batch()
    │                       ├─ tokio::spawn ──── [크론 잡 태스크 #2] process_batch()
    │                       └─ tokio::spawn ──── [즉시 실행 태스크 #1] process_batch()
```

---

## 8. 데이터 흐름 요약

CLI 클라이언트가 배치를 선택했을 때 데이터가 이동하는 경로입니다.

```
[CLI 클라이언트 터미널]
    │
    │ "3\n" 입력
    ▼
[클라이언트 프로세스 - CliClientController]
    │ write_all("3\n") → Unix 소켓 전송
    ▼
[서비스 프로세스 - handle_socket_connection]
    │ reader.read_line() → "3" 수신
    │
    ├─ batch_service.run_batch(batch_items[2]) 실행 시작
    │       │
    │       └─ CLI_LOG_TX.scope(Some(log_tx), ...) 안에서:
    │               batch_log!(info, "Processing...") 호출
    │                   │
    │                   ├──► log::info!() → 서비스 파일 로그 / 서비스 stdout
    │                   │
    │                   └──► log_tx.try_send("Processing...")
    │                               │
    │                        log_rx.recv() [로그 포워딩 태스크]
    │                               │
    │                        socket_tx.send("[LOG] Processing...\n")
    │                               │
    │                        socket_rx.recv() [writer 태스크]
    │                               │
    │                        w.write_all("[LOG] Processing...\n".as_bytes())
    │                               │
    │                        Unix 소켓 → 클라이언트로 전송
    │
    └─ run_batch() 완료
            │
    socket_tx.send("[spent_type] Complete.\n")
            │
    writer 태스크 → 클라이언트로 전송
    ▼
[CLI 클라이언트 터미널 출력]
    [LOG] Processing...
    [LOG] Indexing 1000 documents...
    [spent_type] Complete.
```

---

## 관련 파일 참조

| 파일 | 역할 |
|------|------|
| [controller/main_controller.rs](../src/controller/main_controller.rs) | `main_task()` 구현 |
| [service/cli_service_impl.rs](../src/service/cli_service_impl.rs) | `start_socket_server()`, `handle_socket_connection()` 구현 |
| [utils_module/cli_log.rs](../src/utils_module/cli_log.rs) | `CLI_LOG_TX` task-local 선언 |
| [service/batch_service_impl.rs](../src/service/batch_service_impl.rs) | `batch_log!()` 매크로, `process_batch()` 구현 |
| [service_trait/cli_service.rs](../src/service_trait/cli_service.rs) | `CliService` 트레이트 정의 |
