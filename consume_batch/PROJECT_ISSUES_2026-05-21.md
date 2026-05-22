# 프로젝트 전체 점검 리포트

분석 일자: 2026-05-21  
대상: `consume_batch` Rust 배치 서비스 전체  
작성 파일: `PROJECT_ISSUES_2026-05-21.md`

## 검증 결과

- `cargo test`: 통과. `1 passed`, 컴파일 경고 20개 발생.
- `cargo clippy --all-targets --all-features`: 실패 없이 종료. 경고 29개 발생.
- 기존 `RISK_ANALYSIS.md`는 일부 항목이 이미 수정되었거나 코드 위치가 달라져, 오늘 기준으로 새 파일에 다시 정리했다.
- `.env` 값 자체는 보고서에 적지 않았다. 로컬 파일에는 실제 인증 정보가 있으므로 공유 시 주의가 필요하다.

## 심각도 기준

- Critical: 데이터 유실, 잘못된 alias 전환, 복구 어려운 장애 가능성
- High: 데이터 불일치, 운영 중단, 보안상 큰 위험
- Medium: 부분 실패 은폐, 잘못된 집계, 장애 대응 어려움
- Low: 유지보수성, 설정 혼동, 경고/품질 이슈

## Critical

### 1. Kafka auto commit으로 처리 실패 이벤트가 유실될 수 있음

- 위치: `src/repository/kafka_repository.rs:513-514`
- 관련 처리: `src/service/indexing_service_impl/spent_detail.rs:558-565`
- 현재 consumer 설정이 `enable.auto.commit=true`다. 메시지를 가져온 뒤 MySQL upsert나 Elasticsearch bulk 처리에서 실패해도 offset은 자동 commit될 수 있다.
- 결과적으로 재시작해도 실패한 메시지를 다시 읽지 못해 ES/MySQL 데이터가 조용히 어긋날 수 있다.
- 권장 조치: `enable.auto.commit=false`로 두고, DB/ES 처리 성공 이후 수동 commit한다. 이미 ES `_id=spent_idx`를 쓰는 구조이므로 멱등 upsert와 재시도 큐를 같이 설계하면 좋다.

### 2. Full indexing 시작 시 consumer group offset 복사 실패를 무시하고 계속 진행함

- 위치: `src/service/indexing_service_impl/spent_detail.rs:432-443`
- full indexing은 기존 incremental group offset을 기준으로 catch-up을 맞추려는 구조인데, offset 복사가 실패해도 로그만 남기고 계속 진행한다.
- 이 상태에서 새 index를 만들고 catch-up/alias swap까지 가면, 누락 이벤트가 생기거나 lag 계산이 잘못된 기준으로 수행될 수 있다.
- 권장 조치: offset 복사가 실패하면 full indexing을 중단하고 새 index를 정리하도록 `return Err(e)` 처리한다.

### 3. Consumer group 비활성화가 실제로 수행되지 않는데 안전한 offset 복사처럼 동작함

- 위치: `src/repository/kafka_repository.rs:564-591`
- `is_consumer_group_active`는 항상 `Ok(true)`를 반환하고, `modify_consumer_group_deactivate`는 경고만 출력한 뒤 `Ok(())`를 반환한다.
- active consumer가 있는 상태에서 target group offset을 복사하면 consumer가 곧바로 다른 offset을 commit해 복사 결과를 덮어쓸 수 있다.
- 권장 조치: 실제 group member 확인/중지 절차를 운영 락으로 강제하거나, 보장할 수 없으면 offset copy를 실패 처리한다.

### 4. alias 기준이 섞여 오래된 index가 남거나 잘못 삭제될 수 있음

- 위치: `src/service/indexing_service_impl/spent_detail.rs:236-237`, `src/service/indexing_service_impl/spent_detail.rs:320-345`, `src/service/indexing_service_impl/spent_detail.rs:509-525`
- catch-up에서는 `write_{index}`와 `read_{index}` alias를 새 index로 바꾸고, 이후 full flow에서 다시 기본 alias(`index_name`)도 새 index로 바꾼다.
- 그런데 삭제 대상 old index는 기본 alias 기준으로만 찾는다. 기존 운영이 `read_`/`write_` alias 중심이면 old index 목록이 비어 오래된 index가 남을 수 있다.
- 권장 조치: alias 정책을 하나로 정리한다. 삭제 전에는 기본/read/write alias가 가리키던 index를 모두 수집하고, 새 index는 삭제 대상에서 제외한다.

## High

### 5. `spent_type` full indexing 실패 시 새로 만든 index가 orphan으로 남음

- 위치: `src/service/indexing_service_impl/spent_type.rs:43-100`
- `spent_detail` full flow는 실패 시 `delete_orphaned_index`를 호출하지만, `spent_type` full flow에는 같은 cleanup이 없다.
- bulk index 또는 alias swap 직전 단계에서 실패하면 `spent_type_dev_YYYYMMDDHHMMSS` 형태의 새 index가 남는다.
- 권장 조치: `spent_detail`처럼 단계별 `match`로 감싸고 실패 시 생성된 index를 삭제한다.

### 6. Elasticsearch URL을 항상 `http://`로 강제함

- 위치: `src/repository/es_repository.rs:335-339`
- `ES_DB_URL`에 `https://...`를 넣어도 `http://https://...` 형태가 되어 깨질 수 있고, 현재 구조는 TLS 사용을 사실상 막는다.
- 지출 데이터, 사용자 ID, 카드 별칭 등이 ES로 전송되는 서비스라 평문 통신은 운영 보안 위험이 크다.
- 권장 조치: 설정값에 scheme이 있으면 그대로 사용하고, 없으면 기본값을 명확히 정한다. 운영은 HTTPS와 인증서 검증을 기본으로 둔다.

### 7. MySQL 연결 실패가 `anyhow::Result` 대신 panic으로 종료됨

- 위치: `src/repository/mysql_repository.rs:249-252`
- `MysqlRepositoryImpl::new()`는 `anyhow::Result<Self>`를 반환하지만 내부에서 `expect`를 사용한다.
- 연결 실패 시 호출자가 에러를 처리할 기회 없이 프로세스가 즉시 panic으로 종료된다.
- 권장 조치: `Database::connect(db_url).await.map_err(...) ?`로 바꿔 `main`의 에러 경로에서 로깅/종료하게 한다.

### 8. 타입 재분류 배치에서 조회 재시도 소진 후에도 성공처럼 종료될 수 있음

- 위치: `src/service/batch_service_impl/type_update.rs:91-101`, `src/service/batch_service_impl/type_update.rs:243-253`
- DB 조회가 3회 모두 실패하면 에러 로그를 남긴 뒤 `break`한다. 이후 함수는 완료 로그를 찍고 `Ok(())`로 끝난다.
- 일부 offset까지만 처리된 상태가 정상 완료처럼 보일 수 있다.
- 권장 조치: 재시도 소진 시 `break`가 아니라 `return Err(...)`로 배치 실패를 상위에 전달한다.

### 9. 현재 자산 총액 snapshot에서 예금/적금이 항상 0으로 저장됨

- 위치: `src/service/batch_service_impl/asset.rs:309-333`
- `deposit_amount`, `saving_amount`가 TODO 상태로 `Decimal::ZERO`를 저장한다.
- 이 배치 결과를 사용자 총자산으로 사용한다면 실제 자산보다 낮게 표시된다.
- 권장 조치: 아직 미구현이면 컬럼명을 `known_*`처럼 분리하거나, 예금/적금 집계가 구현되기 전에는 해당 배치를 비활성화한다.

## Medium

### 10. 가격 동기화 API 호출이 순차 실행되고 재시도/timeout 정책이 없음

- 위치: `src/service/batch_service_impl/asset.rs:66-79`, `src/api/twelve_data_api.rs:87-107`
- 종목이 많아질수록 배치 시간이 선형으로 늘고, 일시적인 외부 API 실패가 그대로 실패 카운트로만 누적된다.
- 명시적 request timeout이 없어 외부 API 지연 시 배치가 오래 붙잡힐 수 있다.
- 권장 조치: 명시적 timeout, 제한된 동시성, API rate limit에 맞춘 retry/backoff를 추가한다.

### 11. Twelve Data 요청 URL을 문자열 조합으로 만들고 query encoding을 하지 않음

- 위치: `src/api/twelve_data_api.rs:48-54`, `src/api/twelve_data_api.rs:90-95`
- symbol에 `/`, 공백, 특수 문자가 들어가면 URL 의미가 달라지거나 요청이 깨질 수 있다.
- 권장 조치: `reqwest`의 `.query(&[("symbol", symbol), ("apikey", key)])` 형태를 사용한다.

### 12. 리포트 기간 계산이 UTC 09:00 기준으로 하드코딩되어 있음

- 위치: `src/service/batch_service_impl/report.rs:222-239`, `src/service/batch_service_impl/report.rs:577-591`
- 월간/주간 리포트 시작 시각이 `DateTime<Utc>`의 날짜에 `09:00:00`을 붙이는 방식이다.
- 실제 기준이 한국 시간이라면 하루 경계가 어긋날 수 있다.
- 권장 조치: `chrono-tz`로 `Asia/Seoul` 기준 기간을 만든 뒤 UTC로 변환해 ES range query에 넘긴다.

### 13. 리포트 일부 발송 실패가 배치 실패로 전파되지 않음

- 위치: `src/service/batch_service_impl/report.rs:556-574`
- agg group 작업 중 실패가 있으면 `failed_count`만 증가시키고 최종 반환은 `Ok(())`다.
- CLI/스케줄러 입장에서는 “완료”로 보일 수 있어 재발송이나 알림이 누락될 수 있다.
- 권장 조치: 실패 건수가 0보다 크면 `Err`로 반환하거나, 실패 목록을 별도 테이블/토픽에 기록한다.

### 14. 카테고리 비율 계산에서 총액 0 케이스가 명확하지 않음

- 위치: `src/service/batch_service_impl/report.rs:294`, `src/service/batch_service_impl/report.rs:309`
- `spent_cost / total_cost` 계산 전에 `total_cost == 0` 검사가 없다.
- 지출과 환불이 상쇄되거나 집계 총액이 0인데 양수 카테고리가 남는 케이스에서 `inf`/비정상 비율이 나올 수 있다.
- 권장 조치: 총액이 0 이하이면 비율 계산을 건너뛰거나 0.0으로 고정한다.

## Low / 운영 설정

### 15. `enabled=true`인데 cron/immediate가 모두 false인 스케줄이 많음

- 위치: `datas/batch_schedule.toml:1-125`, `datas/batch_schedule.toml:142-156`
- `enabled=true`인 대부분 배치가 `cron_schedule_apply=false`, `immediate_apply=false`라 서비스 모드에서는 자동 실행되지 않는다.
- 의도한 CLI 전용 설정이면 괜찮지만, 운영자가 `enabled=true`만 보고 실행된다고 착각하기 쉽다.
- 권장 조치: `enabled`와 실행 방식을 분리한 주석/검증을 추가하거나, 시작 시 “enabled but inactive” 항목을 warn으로 출력한다.

### 16. 설정 시스템이 `AppConfig`와 `config::environment::ENV`로 이중화되어 있음

- 위치: `src/app_config.rs:50-87`, `src/config/environment.rs:308-347`
- 실제 main flow는 `AppConfig`를 쓰지만, `ENV`는 별도로 Telegram token까지 요구하며 초기화 실패 시 panic한다.
- 지금은 거의 사용되지 않아 보이지만, 누군가 `ENV`를 가져다 쓰면 기존 `.env` 구성과 다른 요구사항 때문에 런타임 panic이 날 수 있다.
- 권장 조치: 하나의 설정 시스템으로 통합하거나, `ENV`를 제거/비활성화한다.

### 17. `.env`에 공백 포함 key와 실제 비밀값이 함께 존재함

- 위치: `.env:37-49`, `../.gitignore:1`
- parent `.gitignore`에서 `*.env`를 무시하므로 Git 추적 위험은 낮다.
- 다만 `TWELVE_DATA_API = ...`처럼 key와 `=` 사이에 공백이 있어 `dotenv` 외의 loader(shell source, 일부 Docker env_file 파서)에서는 다르게 동작할 수 있다.
- SMTP password/API key가 로컬 평문 파일에 있으므로 공유/백업/로그 수집에 노출되지 않게 관리해야 한다.
- 권장 조치: 공백 없는 `KEY=value` 형식으로 정리하고, `.env.example`에는 key 이름만 남긴다.

### 18. Clippy/컴파일 경고가 누적되어 있음

- 결과: `cargo test`에서 경고 20개, `cargo clippy --all-targets --all-features`에서 경고 29개.
- 주로 unused import, unused variable, unused model, `too_many_arguments` 경고다.
- 당장 장애를 만들지는 않지만 실제 경고가 묻히기 쉬워진다.
- 권장 조치: unused 항목은 정리하고, 의도된 미사용 코드는 `_` prefix 또는 `#[allow(...)]` 범위를 최소화한다.

## 우선순위 제안

1. Kafka auto commit 제거와 수동 commit 처리.
2. full indexing offset copy 실패 시 즉시 중단.
3. alias 정책을 read/write/base 중 하나로 정리하고 old index 삭제 기준 보강.
4. `spent_type` full indexing orphan cleanup 추가.
5. MySQL `expect` 제거, 타입 재분류 배치의 실패 전파 수정.
6. ES HTTPS 설정 지원.
7. 리포트/자산 배치의 집계 정확도와 실패 전파 정책 정리.
