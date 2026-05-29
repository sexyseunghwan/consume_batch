# consume_batch 프로젝트 점검 리포트

점검일: 2026-05-29  
대상: `consume_batch` Rust 배치 서비스 전체  
작성 파일: `PROJECT_REVIEW_2026-05-29.md`

## 검증 결과

- `cargo check --all-targets`: 통과, 경고 35개.
- `cargo test`: 통과, `1 passed`, 경고 35개.
- `cargo clippy --all-targets --all-features`: 통과, 타깃별 경고 44개.
- `.env`의 실제 값은 읽지 않았다. Git ignore 여부와 파일 존재만 확인했다.
- 현재 워크트리에는 기존 변경이 있다: `Cargo.toml`, `Cargo.lock`, `src/service/batch_service_impl/asset.rs` 수정, `PROJECT_ISSUES_2026-05-21.md` 삭제.

## 심각도 기준

- Critical: 데이터 유실, 잘못된 색인 전환, 복구 어려운 운영 장애 가능성.
- High: 운영 중단, 데이터 불일치, 보안상 큰 위험.
- Medium: 부분 실패 은폐, 잘못된 집계, 장애 대응 난이도 증가.
- Low: 유지보수성, 설정 혼동, 경고/품질 이슈.

## Critical

### 1. Kafka auto commit 때문에 처리 실패 이벤트가 유실될 수 있음

- 위치: `src/repository/kafka_repository/consumer.rs:44-50`
- 관련 처리: `src/service/indexing_service_impl/spent_detail.rs:149-223`
- 현재 Kafka consumer가 `enable.auto.commit=true`로 생성된다. 메시지를 읽은 뒤 MySQL upsert, ES bulk index/delete 중 하나가 실패해도 offset이 먼저 commit될 수 있다.
- 결과적으로 재시작해도 실패한 메시지를 다시 읽지 못해 MySQL/Elasticsearch 상태가 조용히 어긋날 수 있다.

해결 가이드:
- `enable.auto.commit=false`로 변경한다.
- 메시지 poll 시 topic/partition/offset metadata를 함께 보관한다.
- DB/ES 처리가 모두 성공한 뒤에만 해당 offset을 수동 commit한다.
- 실패 시에는 같은 메시지를 재처리할 수 있게 두고, 반복 실패는 retry limit + DLQ 또는 별도 실패 테이블로 보낸다.
- ES `_id=spent_idx` 구조는 멱등 upsert에 유리하므로, 재처리를 전제로 테스트를 추가한다.

### 2. Full indexing의 offset 복사 실패를 무시하고 계속 진행함

- 위치: `src/service/indexing_service_impl/spent_detail.rs:432-443`
- full indexing은 incremental group offset을 복사한 뒤 catch-up을 수행하는 구조다. 그런데 offset 복사가 실패해도 로그만 찍고 새 index 생성, full load, catch-up, alias swap까지 계속 진행한다.
- offset 기준이 잘못되면 신규 index에 일부 변경분이 누락되거나, lag 계산이 잘못된 기준으로 완료될 수 있다.

해결 가이드:
- offset 복사가 실패하면 즉시 `return Err(e)`로 full indexing을 중단한다.
- 이미 만든 신규 index가 있다면 `delete_orphaned_index`로 정리한다.
- `modify_offsets_internal`에서 source group의 offset이 전부 invalid라서 skip하는 경우도 성공으로 보지 말고, 명시적인 운영 옵션이 없으면 실패 처리한다.
- full indexing 시작 전 `source_group`, `target_group`, topic partition별 committed offset snapshot을 로그/운영 테이블에 남긴다.

### 3. Catch-up 실패 시 incremental indexing이 영구 정지될 수 있음

- 위치: `src/service/indexing_service_impl/spent_detail.rs:293-300`, `src/service/indexing_service_impl/spent_detail.rs:362-374`, `src/service/indexing_service_impl/spent_detail.rs:467-479`
- catch-up은 lag가 batch size 이하가 되면 전역 플래그 `SPENT_DETAIL_INDEXING`을 `false`로 바꿔 incremental loop를 멈춘다.
- 이후 catch-up에서 연속 오류가 발생하면 `Err`로 빠져나오지만, `set_spent_detail_indexing(true)`는 alias swap 성공 경로에서만 호출된다.
- 이 경우 full indexing은 실패했는데 incremental indexing도 계속 멈춰 서비스가 새 이벤트를 처리하지 못할 수 있다.

해결 가이드:
- pause/resume을 guard 구조로 감싼다. 예: `IndexingPauseGuard`를 만들고 `Drop` 또는 명시적 `finish()`로 실패 경로에서도 반드시 resume한다.
- `modify_spent_detail_catch_up`에서 `indexing_paused == true`인 상태로 반환하는 모든 경로를 테스트한다.
- 전역 bool 대신 `tokio::sync::watch`나 상태 enum(`Running`, `PausedForCatchup`, `Recovering`)으로 관측 가능한 상태를 만든다.
- 동일 index에 full/incremental/CLI 실행이 겹치지 않도록 index별 async lock을 추가한다.

### 4. 현재 자산 총액 배치가 조회만 하고 snapshot을 저장하지 않음

- 위치: `src/service/batch_service_impl/asset.rs:269-379`
- `sync_current_asset_total`은 주식, 코인, 현금, 예금, 적금 금액을 모두 조회하지만 snapshot 생성과 `input_user_current_asset_snapshot_bulk` 호출이 통째로 주석 처리되어 있다.
- 그런데 로컬 `datas/batch_schedule.toml`에서는 이 배치가 `immediate_apply=true`로 되어 있어, 서비스 시작 시 실행되어도 아무 snapshot을 남기지 않는다.
- 컴파일 경고도 이 문제를 드러낸다: `stock_map`, `crypto_map`, `cash_map`, `deposit_map`, `saving_map`, `now`, `zero`가 모두 unused다.

해결 가이드:
- 의도적으로 막아둔 상태라면 스케줄에서 `enabled=false` 또는 `immediate_apply=false`로 내려 운영자가 성공으로 오해하지 않게 한다.
- 기능을 살릴 예정이면 주석 처리된 snapshot 생성 코드를 복구하고, `input_user_current_asset_snapshot_bulk` 호출까지 재활성화한다.
- `currency_code`별로 동일 user snapshot이 중복 저장되어도 되는지 정책을 정한다. 이력이 목적이면 insert, 최신값만 필요하면 unique key + upsert로 간다.
- 이 배치는 금액 데이터라 회귀 테스트를 붙이는 편이 좋다. 최소한 `to_amount_map`과 user별 default zero 처리 테스트를 추가한다.

### 5. Consumer group 비활성화가 실제로 보장되지 않음

- 위치: `src/repository/kafka_repository/admin.rs:38-72`
- `is_consumer_group_active`는 항상 `Ok(true)`를 반환하고, `modify_consumer_group_deactivate`는 경고만 남긴 뒤 `Ok(())`를 반환한다.
- active consumer가 target group offset을 계속 commit하는 상태에서 offset copy를 수행하면 복사 결과가 곧바로 덮일 수 있다.

해결 가이드:
- 실제 group member 조회가 가능하도록 Kafka Admin API 버전 또는 보조 CLI 절차를 도입한다.
- target group이 active면 offset copy를 실패 처리한다.
- 운영 절차상 애플리케이션을 멈춰야 한다면 코드에서 명확히 `Err`를 반환하고, runbook에 "중지 후 실행" 절차를 둔다.
- batch 실행 전/후 target group offset을 다시 읽어 복사 결과가 유지되는지 검증한다.

## High

### 6. `spent_type` full indexing 실패 시 orphan index가 남음

- 위치: `src/service/indexing_service_impl/spent_type.rs:43-130`
- `spent_detail` full flow는 실패 시 신규 index를 삭제하지만, `spent_type` full flow에는 같은 cleanup이 없다.
- bulk index, setting 변경, alias swap 중 실패하면 `spent_type_YYYYMMDDHHMMSS` 형태의 신규 index가 남을 수 있다.

해결 가이드:
- `spent_detail`과 동일하게 신규 index 생성 이후 단계별 `match`를 두고 실패 시 `delete_orphaned_index(&new_index_name)`를 호출한다.
- alias swap 성공 이후 old index 삭제 실패는 운영자가 재시도할 수 있게 old index 목록을 로그에 남긴다.
- `spent_type`과 `spent_detail` full indexing 공통 템플릿을 만들되, 과도한 일반화보다 cleanup 보장에 초점을 둔다.

### 7. base/read/write alias 정책이 섞여 index 정리가 불안정함

- 위치: `src/service/indexing_service_impl/spent_detail.rs:235-237`, `src/service/indexing_service_impl/spent_detail.rs:320-345`, `src/service/indexing_service_impl/spent_detail.rs:509-525`
- catch-up에서는 `write_{index}`와 `read_{index}` alias를 새 index로 바꾸고, full flow 끝에서는 기본 alias도 새 index로 바꾼다.
- 삭제 대상 old index는 기본 alias 기준으로만 수집한다. 운영이 read/write alias 중심이면 오래된 index가 남을 수 있고, alias 정책을 모르는 사람이 잘못된 alias를 쓸 위험이 있다.

해결 가이드:
- 검색/쓰기/관리 alias를 명확히 분리한다. 예: 검색은 `read_spent_detail`, 쓰기는 `write_spent_detail`, 관리용 base alias는 제거 또는 문서화.
- old index 삭제 전 base/read/write alias가 가리키던 index를 모두 수집하고, 새 index는 삭제 대상에서 제외한다.
- alias swap은 한 번의 `_aliases` 요청으로 atomic하게 처리하는 쪽이 안전하다.
- alias 상태를 검증하는 smoke test 또는 운영 점검 명령을 문서화한다.

### 8. Elasticsearch URL을 항상 `http://`로 강제함

- 위치: `src/repository/es_repository.rs:335-340`
- `ES_DB_URL`에 `https://host:9200`을 넣으면 `http://https://host:9200`이 되어 깨진다.
- 운영에서 HTTPS를 쓰고 싶어도 현재 구조는 평문 HTTP를 사실상 강제한다.

해결 가이드:
- host 값이 `http://` 또는 `https://`로 시작하면 그대로 사용하고, scheme이 없을 때만 기본값을 붙인다.
- 운영 기본은 HTTPS로 두고, 로컬 개발만 HTTP를 허용한다.
- 인증서 검증 옵션이 필요하면 명시적인 설정값으로 분리한다.

### 9. MySQL 연결 실패가 `Result`가 아니라 panic으로 종료됨

- 위치: `src/repository/mysql_repository.rs:249-252`
- `MysqlRepositoryImpl::new()`는 `anyhow::Result<Self>`를 반환하지만 내부에서 `expect`를 사용한다.
- 연결 실패 시 호출자가 에러를 로깅하거나 graceful shutdown할 기회가 없다.

해결 가이드:
- `Database::connect(db_url).await.map_err(|e| anyhow!(...))?` 형태로 바꾼다.
- `main`에서는 panic 대신 에러 로그 후 `Err`를 반환하도록 통일한다.
- Elasticsearch/Kafka/MySQL 초기화 실패 정책을 하나로 맞춘다.

### 10. 타입 재분류 배치가 조회 재시도 실패 후에도 성공처럼 끝날 수 있음

- 위치: `src/service/batch_service_impl/type_update.rs:91-102`, `src/service/batch_service_impl/type_update.rs:243-254`
- DB 조회가 모든 retry에서 실패하면 에러 로그를 남기고 `break`한다.
- 이후 함수는 남은 데이터를 처리하지 않았는데도 정상 종료될 수 있다.

해결 가이드:
- retry 소진 시 `break` 대신 `return Err(anyhow!(...))`로 상위에 실패를 전파한다.
- `last_err.unwrap()` 대신 `last_err.context(...)` 방식으로 panic 여지를 없앤다.
- 처리 offset, 총 처리 건수, 실패 offset을 운영 테이블이나 로그에 남겨 재개할 수 있게 한다.

### 11. 스케줄 설정 파일과 코드 계약이 어긋남

- 위치: `src/models/batch_schedule.rs:84-90`, `datas/batch_schedule_prod.toml:1-64`, `src/service/batch_service_impl/dispatcher.rs:42-174`
- `BatchScheduleItem`은 `consumer_group_sub`를 필수 필드로 요구한다.
- 로컬 `datas/batch_schedule.toml`에는 값이 있지만, `datas/batch_schedule_prod.toml`과 `datas/batch_schedule_test.toml`에는 여러 항목에서 `consumer_group_sub`가 없다. 해당 파일을 쓰면 TOML deserialize 단계에서 실패할 수 있다.
- prod/test 파일에는 `spent_detail_migration_to_kafka`가 있지만 dispatcher에는 해당 batch_name 분기가 없어 실행해도 unknown으로 skip된다.

해결 가이드:
- 선택 필드는 `#[serde(default)]`를 붙이거나 `Option<String>`으로 바꾼다.
- 모든 스케줄 파일을 대상으로 deserialize 테스트를 추가한다.
- dispatcher의 batch_name은 enum으로 만들고 TOML도 enum deserialize를 쓰면 오타/미구현 배치를 시작 시점에 잡을 수 있다.
- `enabled=true`이지만 `cron_schedule_apply=false`이고 `immediate_apply=false`인 항목은 startup warning으로 출력한다.

## Medium

### 12. 외부 API 호출에 timeout, retry, rate limit 정책이 약함

- 위치: `src/service/batch_service_impl/asset.rs:29-95`, `src/api/twelve_data_api.rs:45-114`
- 가격 동기화는 종목별 API 호출을 순차 실행한다.
- `reqwest::Client` 기본 설정을 사용해 명시적 request timeout이 없고, 일시 장애 retry/backoff도 없다.
- URL도 문자열 조합으로 만들고 있어 symbol에 `/`, 공백, 특수 문자가 들어가면 요청 의미가 달라질 수 있다.

해결 가이드:
- `Client::builder().timeout(...).build()`로 명시 timeout을 둔다.
- `reqwest`의 `.query(&[("symbol", symbol), ("apikey", key)])`를 사용한다.
- `futures::stream::iter(...).buffer_unordered(N)` 같은 제한된 동시성을 도입한다.
- Twelve Data rate limit에 맞춰 retry/backoff와 실패 카운트 알림을 둔다.

### 13. 리포트 기간 계산이 UTC 09:00 기준으로 하드코딩됨

- 위치: `src/service/batch_service_impl/report.rs:222-239`, `src/service/batch_service_impl/report.rs:577-591`
- 월간/주간 리포트 시작 시각을 `DateTime<Utc>` 날짜에 `09:00:00`으로 붙인다.
- 실제 기준이 한국 시간이라면 KST 09:00이 아니라 UTC 09:00, 즉 KST 18:00 기준이 된다.

해결 가이드:
- `chrono-tz`의 `Asia/Seoul`로 기간 경계를 먼저 만든다.
- ES range query에는 최종적으로 UTC로 변환한 값을 넘긴다.
- "최근 1개월"인지 "지난 달 1일~말일"인지 리포트 상품 정의도 분리해 명확히 테스트한다.

### 14. 리포트 일부 발송 실패가 배치 실패로 전파되지 않음

- 위치: `src/service/batch_service_impl/report.rs:556-574`
- agg group task 실패가 있어도 `failed_count`를 warn으로만 남기고 최종 반환은 `Ok(())`다.
- CLI/스케줄러 입장에서는 성공한 배치처럼 보일 수 있어 재발송이 누락된다.

해결 가이드:
- `failed_count > 0`이면 `Err`를 반환하거나, 실패 목록을 별도 테이블에 남겨 재시도 배치가 처리하게 한다.
- 메일 발송 결과를 recipient 단위로 기록한다.
- 부분 성공을 허용한다면 성공/실패 수를 배치 결과 모델로 반환한다.

### 15. 카테고리 비율 계산에서 총액 0 케이스가 없음

- 위치: `src/service/batch_service_impl/report.rs:286-301`
- `spent_cost / total_cost` 계산 전에 `total_cost == 0` 검사가 없다.
- 지출과 취소/환불이 상쇄되거나 집계값이 0인 상태에서 양수 카테고리가 남으면 `inf` 또는 비정상 비율이 나올 수 있다.

해결 가이드:
- `total_cost <= 0.0`이면 비율 계산을 건너뛰거나 모두 0.0으로 고정한다.
- 집계 총액은 ES aggregation 결과가 아니라 category map 합계와 비교 검증한다.
- 환불/마이너스 지출을 리포트에서 어떻게 표현할지 정책을 정한다.

### 16. ES index shard/replica 수가 코드에 하드코딩됨

- 위치: `src/service/elastic_service_impl/insert.rs:57-69`, `src/service/elastic_service_impl/update.rs:80-91`
- full indexing 중에는 shard 3, replica 0, refresh -1로 덮고, 운영 설정은 replica 2, refresh 1s로 고정한다.
- 로컬/개발/단일 노드 환경에서는 replica 2 때문에 yellow cluster가 될 수 있고, 운영 index별 요구사항도 반영하기 어렵다.

해결 가이드:
- mapping schema JSON 또는 batch schedule에 `indexing_settings`와 `production_settings`를 분리한다.
- 환경별 기본값을 `.env` 또는 TOML로 뺀다.
- setting 변경 후 cluster health를 확인하고 alias swap 전에 green/yellow 허용 정책을 명시한다.

### 17. 설정 시스템이 `AppConfig`와 `config::environment::ENV`로 이중화됨

- 위치: `src/app_config.rs:48-99`, `src/config/environment.rs:308-354`
- 실제 main flow는 `AppConfig`를 사용하지만, `ENV`는 별도로 Telegram token 등을 요구하고 초기화 실패 시 panic한다.
- 누군가 `ENV`를 새 코드에서 사용하면 기존 `.env` 요구사항과 달라 런타임 장애가 날 수 있다.

해결 가이드:
- 하나의 설정 시스템으로 통합한다.
- 사용하지 않는 `config::environment`는 제거하거나, 정말 필요한 경우 `AppConfig`와 같은 필드/기본값 정책을 공유한다.
- 설정 로딩은 panic이 아니라 `Result`로 전파한다.

### 18. CLI 소켓 파일 제거가 경로 오설정에 취약함

- 위치: `src/service/cli_service_impl.rs:211-221`
- 서비스 시작 시 `socket_path`를 바로 `remove_file`한다.
- `SOCKET_PATH`가 실수로 일반 파일 경로를 가리키면 해당 파일을 삭제할 수 있다.

해결 가이드:
- 삭제 전 `symlink_metadata`로 파일 타입이 Unix socket인지 확인한다.
- socket directory를 고정하거나 allowlist한다.
- socket path parent directory가 없으면 생성하고, 일반 파일이면 실패 처리한다.

## Low / 유지보수

### 19. 경고가 많아 실제 위험 신호가 묻힘

- 결과: `cargo check`/`cargo test` 경고 35개, `cargo clippy` 경고 44개.
- 특히 `asset.rs`의 unused 변수들은 실제 미완성 기능과 연결되어 있다.

해결 가이드:
- 사용하지 않는 import와 model re-export를 정리한다.
- 의도된 미사용 코드는 `_` prefix 또는 최소 범위의 `#[allow(...)]`만 사용한다.
- CI에서 `cargo clippy --all-targets --all-features -- -D warnings`를 바로 켜기 어렵다면, 먼저 `asset.rs`와 신규 코드만 경고 0개로 맞춘다.

### 20. 빌드 산출물과 운영 설정의 버전 관리 정책이 섞여 있음

- 위치: Git 추적 파일 `consume_batch_v1`, parent `.gitignore`
- 바이너리 `consume_batch_v1`은 Git에 추적되어 있다.
- 반면 `datas/`는 ignore되어 있어 로컬 batch schedule과 report template은 배포/리뷰에서 누락되기 쉽다.

해결 가이드:
- 바이너리는 Git에서 제거하고 릴리스 산출물 또는 CI artifact로 관리한다.
- `datas/*.example.toml`, `datas/scripts/report.example.html`처럼 비밀값 없는 샘플 설정은 추적한다.
- 실제 운영 설정은 secret/config management로 분리한다.

### 21. 문서와 실제 함수명이 일부 어긋남

- 위치: `docs/socket_server_flow.md`, `docs/cli_architecture.md`, 실제 코드 `src/service/cli_service_impl.rs`
- 문서에는 과거 이름인 `start_socket_server`, `handle_socket_connection`이 남아 있고 실제 코드는 `initialize_socket_server`, `find_socket_session`이다.

해결 가이드:
- 함수명 변경 시 docs도 함께 수정한다.
- 아키텍처 문서는 코드에서 참조하는 실제 이름 위주로 유지한다.

## 권장 작업 순서

1. Kafka consumer를 수동 commit 구조로 바꾸고 실패 재처리 테스트를 추가한다.
2. full indexing offset copy 실패 시 즉시 중단하고, catch-up pause guard를 추가한다.
3. `sync_current_asset_total`을 완성하거나 스케줄에서 비활성화한다.
4. consumer group active 상태를 보장하지 못하면 offset copy를 실패 처리한다.
5. `spent_type` orphan cleanup과 alias 정책 정리를 진행한다.
6. MySQL `expect` 제거, 타입 재분류 배치 실패 전파 수정, ES HTTPS 지원을 묶어 운영 안정성 패치로 처리한다.
7. 스케줄 파일 deserialize 테스트와 batch_name enum화를 추가한다.
8. 리포트/외부 API/설정 이중화/경고 정리는 그 다음 단계에서 품질 개선 패치로 나눈다.

