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

## 4. 소켓 통신 프로토콜

```
[CLI 클라이언트]                          [서비스 - CliServiceImpl]
      │                                          │
      │  ←─── 메뉴 전송 (Input:\n 으로 끝남) ────    │
      │                                          │
      │  ──── "3\n" 전송 ────────────────────►    │
      │                                          ├─ "3" 파싱 → batch_items[2]
      │                                          ├─ info!("CLI triggered: spent_type")
      │  ←─── "[spent_type] Batch execution..." ─│
      │                                          ├─ batch_service.run_batch(item).await
      │                                          │    └─ process_batch() 실행
      │                                          │         ├─ MySQL 쿼리
      │                                          │         ├─ ES 인덱싱
      │                                          │         └─ Kafka produce/consume
      │  ←─── "[spent_type] Complete.\n" ────────│
      │                                          │
      │  ←─── 다음 메뉴 전송 ───────────────────────│
```

**read_until_prompt() 동작:**
클라이언트는 `BufReader::read_line()`으로 서버 메시지를 한 줄씩 읽어 출력합니다.
`"Input:\n"`이 포함된 줄을 받으면 루프를 탈출하고 사용자 입력을 받습니다.
(`\n` 없이 `"Input:"` 만 보내면 read_line이 영원히 대기하는 데드락 발생)

---

## 5. 로그 출력 위치

| 항목 | 위치 |
|------|------|
| 배치 실행 상세 로그 | `logs/consume_batch_v1__rCURRENT.log` (파일) |
| 배치 실행 상세 로그 | 서비스를 실행한 터미널 stdout (`duplicate_to_stdout`) |
| CLI 클라이언트 출력 | `--cli` 터미널 (메뉴, "Complete.", "Failed:" 메시지만) |

---

## 6. 파일별 역할 요약

| 파일 | 역할 |
|------|------|
| `main.rs` | 진입점. 모드 분기 및 의존성 주입 |
| `controller/main_controller.rs` | BatchService + CliService 조합 및 태스크 실행 |
| `controller/cli_client_controller.rs` | CLI 클라이언트 (소켓 연결, 메뉴 출력, 입력 전송) |
| `service/cli_service_impl.rs` | 소켓 서버 (메뉴 전송, 입력 수신, 배치 실행) |
| `service_trait/cli_service.rs` | CliService 트레이트 정의 |
| `service/batch_service_impl.rs` | 실제 배치 로직 (MySQL→ES 인덱싱 등) |
| `app_config.rs` | 환경변수 기반 전역 설정 (SOCKET_PATH 포함) |
