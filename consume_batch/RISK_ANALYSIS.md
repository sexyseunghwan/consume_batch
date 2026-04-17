# 프로젝트 위험 요소 분석 (Risk Analysis)

> 분석 일자: 2026-04-15  
> 분석 대상: consume_batch 전체 코드베이스

---

## 심각도 분류

| 심각도 | 기준 |
|---|---|
| 🔴 Critical | 데이터 유실 / 서비스 중단 / 보안 침해 가능 |
| 🟠 High | 성능 저하 / 잘못된 데이터 / 복구 어려운 상태 |
| 🟡 Medium | 부분적 데이터 불일치 / 예외 처리 누락 |
| 🟢 Low | 코드 품질 / 운영 불편 |

---

## 🔴 Critical

---

### 1. Kafka auto-commit으로 메시지 유실 가능

**파일**: `src/repository/kafka_repository.rs`  
**위험**: 메시지 처리 실패 시 offset이 이미 commit되어 해당 이벤트 영구 유실

**원인**:  
Kafka consumer 설정에 `enable.auto.commit=true`가 적용되어 있다.
메시지를 consume한 뒤 ES/MySQL 처리 도중 실패하더라도 Kafka는 offset을 자동으로 commit한다.
재시작 시 실패한 메시지부터 다시 읽지 않고 다음 메시지부터 읽게 된다.

**증상**: ES와 MySQL 데이터가 맞지 않는데 Kafka에서 재처리할 방법이 없는 상태

**권장 조치**:  
- `enable.auto.commit=false`로 변경
- 처리 완료 후 수동으로 `consumer.commit()`
- 또는 At-least-once 보장을 위한 멱등성(idempotency) 처리 추가

---

### 2. ES Bulk 부분 실패를 warn만 출력하고 계속 진행

**파일**: `src/repository/es_repository.rs` (bulk_index, bulk_update, bulk_delete)  
**위험**: 일부 문서 인덱싱/삭제 실패 시 ES-MySQL 데이터 불일치가 조용히 발생

**원인**:  
Elasticsearch의 bulk API는 HTTP 200을 반환하더라도 응답 body 안의 `errors` 필드가 `true`일 수 있다.
현재 코드는 이 경우 `warn!`만 출력하고 `Ok(())`를 반환한다.

```rust
// es_repository.rs
if let Some(errors) = response_body.get("errors")
    && errors.as_bool() == Some(true)
{
    warn!("Some documents failed to index: ...");  // 에러인데 Ok() 반환
}
Ok(())  // <- 여기가 문제
```

**증상**: MySQL에는 있고 ES에는 없는 문서 발생. 어느 문서가 실패했는지 알 수 없음

**권장 조치**:  
- `errors == true`일 때 실패한 항목을 파싱하여 `Err` 반환 또는 재시도 큐에 넣기
- 최소한 실패한 doc_id 목록을 error 레벨로 로깅

---

### 3. Elasticsearch HTTP 평문 통신

**파일**: `src/repository/es_repository.rs:363`  
**위험**: 네트워크 스니핑으로 지출 데이터(금액, 사용처, 카드정보) 전체 노출

**원인**:  
```rust
format!("http://{}", host)  // HTTPS가 아닌 HTTP 사용
```

ES와의 통신이 암호화되지 않아 내부 네트워크라도 패킷 캡처 시 모든 인덱싱 데이터가 평문 노출된다.

**권장 조치**:  
- ES 클러스터에 TLS 설정 후 `https://`로 변경
- 인증서 검증 활성화
- 최소한 내부망 격리 + 방화벽 규칙 확인

---

### 4. Unix 소켓 파일 삭제 실패 시 서버 시작 불가

**파일**: `src/service/cli_service_impl.rs:219`  
**위험**: 소켓 파일이 존재하지 않을 경우(최초 실행) 서버가 시작되지 않음

**원인**:  
```rust
std::fs::remove_file(socket_path).inspect_err(|e| {
    error!(...);
})?;  // <- 파일이 없으면 Err 반환으로 서버 시작 불가
```

`remove_file`은 파일이 없으면 `NotFound` 에러를 반환한다.
최초 실행이나 정상 종료 후 재시작 시, 소켓 파일이 없는 것은 정상인데 에러로 처리된다.

**권장 조치**:  
```rust
// NotFound 에러는 무시
match std::fs::remove_file(socket_path) {
    Ok(_) => {}
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
    Err(e) => return Err(e.into()),
}
```

---

### 5. Catch-up / 증분 루프에서 영구 재시도 상태 가능

**파일**: `src/service/indexing_service_impl.rs` (`process_spent_detail_catch_up`, `run_spent_detail_incremental`)  
**위험**: 처리 불가능한 에러(예: DB down) 발생 시 루프가 끝나지 않고 에러를 계속 재시도

**원인**:  
```rust
// process_spent_detail_catch_up
Err(e) => {
    error!(...);
    continue;  // <- 에러 원인에 무관하게 재시도
}
```

Kafka는 계속 오프셋을 쌓는데 처리는 안 되면서 lag이 무한 증가한다.
에러 종류를 구분하지 않아 일시적 에러와 영구적 에러를 동일하게 처리한다.

**권장 조치**:  
- 연속 실패 횟수를 카운트하여 임계치 초과 시 루프 종료 또는 알림 발송
- 에러 종류(네트워크, DB, 파싱)에 따라 retry 전략 분기

---

## 🟠 High

---

### 6. Bulk 요청 데이터 전체를 info 레벨로 로깅

**파일**: `src/repository/es_repository.rs:627, 712, 769`  
**위험**: 대용량 배치에서 로그 폭발 + 개인정보(지출내역) 로그 파일에 평문 저장

**원인**:  
```rust
info!("[EsRepositoryImpl::bulk_index] {:?}", debug_body);  // 모든 문서 출력
```

1만 건 배치 실행 시 2만 줄의 JSON이 로그에 기록된다.
`spent_name`, `card_alias`, `user_id` 등 개인식별 데이터가 포함된다.

**권장 조치**:  
- `debug_body` 변수 자체를 제거하거나 `debug!` 레벨로 변경
- 로그에는 인덱스명과 문서 수만 출력

---

### 7. debug_body로 인한 이중 메모리 사용

**파일**: `src/repository/es_repository.rs` (bulk_index, bulk_update)  
**위험**: 대용량 배치에서 메모리 사용량이 실제 필요량의 2배

**원인**:  
```rust
let mut body: Vec<JsonBody<_>> = Vec::with_capacity(documents.len() * 2);
let mut debug_body: Vec<Value> = Vec::with_capacity(documents.len() * 2);  // 동일 데이터 복사
```

`debug_body`는 로그 출력 외에 실제로 사용되지 않지만 전체 문서를 복사하여 메모리에 보관한다.

**권장 조치**:  
- `debug_body` 변수 제거

---

### 8. N+1 Elasticsearch 쿼리 패턴

**파일**: `src/service/batch_service_impl.rs:598-607` (`process_update_all_check_type_detail`)  
**위험**: 레코드 수만큼 ES 쿼리 발생으로 대량 데이터 처리 시 심각한 성능 저하 + ES 과부하

**원인**:  
```rust
for detail in &details {
    let spent_type = elastic_service
        .get_consume_type_judgement(detail.spent_name())  // 레코드 1개당 ES 쿼리 1회
        .await?;
}
```

배치 크기가 500이면 루프 1회에 ES 쿼리 500번이 순차적으로 실행된다.

**권장 조치**:  
- `multi-search` API로 배치 내 쿼리를 한 번에 처리
- 또는 `spent_name` 기준으로 중복 제거 후 캐싱 적용

---

### 9. production 설정값 하드코딩

**파일**: `src/service/elastic_service_impl.rs:164`  
**위험**: 단일 노드 환경(개발/테스트)에서 replica 2 설정으로 인덱싱 실패 가능

**원인**:  
```rust
let production_settings: Value = json!({
    "index": {
        "number_of_replicas": 2,  // 하드코딩
        "refresh_interval": "1s"
    }
});
```

단일 노드 ES에서 replica 2는 설정은 적용되지만 실제로는 UNASSIGNED 상태로 클러스터 health가 yellow/red가 된다.

**권장 조치**:  
- `AppConfig`에 `es_number_of_replicas` 환경변수 추가
- 환경별로 값을 다르게 설정할 수 있도록 외부화

--- 여기까지 마침...

### 10. AppConfig::global() 초기화 전 호출 시 panic

**파일**: `src/app_config.rs:112`  
**위험**: 초기화 순서가 틀리면 전체 프로세스 비정상 종료

**원인**:  
```rust
pub fn global() -> &'static AppConfig {
    APP_CONFIG
        .get()
        .expect("AppConfig not initialized...")  // panic
}
```

`AppConfig::init()` 호출 전에 `global()`을 호출하면 panic이 발생한다.
현재는 `main()`에서 순서를 지키고 있지만, 라이브러리 코드나 테스트에서 실수로 먼저 호출하면 크래시.

**권장 조치**:  
- `global()` → `try_global()`로 반환 타입을 `Result`로 변경하거나
- 내부에서 `init()`을 lazy하게 호출하는 패턴 사용

---

## 🟡 Medium

---

### 11. upsert_spent_detail_indexing에 트랜잭션 없음

**파일**: `src/service/mysql_service_impl.rs` (`upsert_spent_detail_indexing`)  
**위험**: 대량 upsert 도중 실패 시 일부만 반영된 중간 상태로 남음

**원인**:  
```rust
spent_detail_indexing::Entity::insert_many(active_models)
    .on_conflict(...)
    .exec_without_returning(db)  // 트랜잭션 없이 직접 실행
    .await?;
```

다른 write 함수들(`delete_spent_detail_indexing`, `insert_dim_calendar_bulk`)은 모두 트랜잭션을 사용하는데 이 함수만 없다.

**권장 조치**:  
- `db.begin()` → 처리 → `txn.commit()`으로 트랜잭션 추가

---

### 12. process_update_all_check_type_detail에서 fetch 실패 시 break

**파일**: `src/service/batch_service_impl.rs:576-586`  
**위험**: MySQL 일시 장애 시 배치가 중간에 중단되고, 이후 레코드가 처리되지 않음

**원인**:  
```rust
let details = match mysql_service.fetch_spent_details(offset, batch_size).await {
    Ok(details) => details,
    Err(e) => {
        batch_log!(error, ...);
        break;  // 재시도 없이 루프 종료
    }
};
```

일시적 네트워크 오류로도 전체 배치 작업이 중단된다.

**권장 조치**:  
- 재시도 로직(exponential backoff) 추가
- 또는 실패한 `offset`을 기록하여 이후 재처리 가능하도록 설계

---

### 13. Full indexing 중 catch-up 실패 시 alias 미전환 상태 방치

**파일**: `src/service/indexing_service_impl.rs` (`run_spent_detail_full`)  
**위험**: 새 인덱스가 생성되었지만 alias는 여전히 구 인덱스를 가리키는 상태로 남을 수 있음

**원인**:  
Step 4(full indexing) 완료 → Step 5(catch-up) 실패 → alias swap이 일어나지 않음.
이 경우 새 인덱스는 삭제되지 않고 방치되며, 다음 full indexing 실행 시 또 다른 새 인덱스가 생성된다.

**권장 조치**:  
- catch-up 실패 시 생성된 새 인덱스를 명시적으로 삭제하는 정리 로직 추가
- 또는 catch-up 재시도 횟수 제한 후 실패 시 정리

---

### 14. 소켓 파일 접근 권한 미설정

**파일**: `src/service/cli_service_impl.rs:227`  
**위험**: Unix 소켓에 퍼미션이 설정되지 않아 서버 호스트의 모든 사용자가 접근 가능

**원인**:  
```rust
let listener = UnixListener::bind(socket_path)?;
// 권한 설정 없음 → umask에 의존
```

기본 umask에 따라 소켓 파일 권한이 결정되며, 의도치 않은 사용자가 CLI 명령을 실행할 수 있다.

**권장 조치**:  
- 소켓 생성 후 `std::fs::set_permissions()`으로 `0o600` 명시 설정
- 또는 소켓 경로를 root만 접근 가능한 디렉토리에 배치

---

## 🟢 Low

---

### 15. 공공데이터포털 API URL 하드코딩

**파일**: `src/service/public_data_service_impl.rs:41`  
**위험**: API URL 변경 시 코드 수정 및 재배포 필요

**원인**:  
```rust
let url = format!(
    "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getHoliDeInfo...",
    ...
);
```

**권장 조치**: `AppConfig`에 `public_data_api_url` 환경변수로 외부화

---

### 16. is_weekend 계산이 공휴일을 고려하지 않음

**파일**: `src/service/batch_service_impl.rs:733`  
**위험**: `DIM_CALENDAR`의 `is_weekend` 값이 순수 요일 기반으로만 계산됨

**원인**:  
```rust
let is_weekend: i8 = if weekday_no >= 5 { 1 } else { 0 };
// is_holiday 여부와 무관하게 요일만 체크
```

공휴일이 평일에 걸려도 `is_weekend = 0`, `is_weekday = 1`로 기록된다.

**권장 조치**:  
- `is_holiday == 1`인 경우 `is_weekday = 0`으로 처리하는 로직 추가 여부 검토
- 또는 `is_holiday`와 `is_weekend`를 분리하여 독립적으로 사용하도록 쿼리 작성 가이드 정리

---

### 17. Kafka SASL 인증 설정이 AppConfig에는 있지만 실제 연결에 미반영 여부 확인 필요

**파일**: `src/app_config.rs`, `src/repository/kafka_repository.rs`  
**위험**: 보안 설정이 환경변수로 주입되지만 실제 Kafka 클라이언트에 적용되지 않으면 평문 통신

**원인**:  
`AppConfig`에 `kafka_security_protocol`, `kafka_sasl_mechanism`, `kafka_sasl_username`, `kafka_sasl_password`가 선언되어 있으나, `KafkaRepositoryImpl` 내부에서 이 값들이 실제로 `ClientConfig`에 설정되는지 확인이 필요하다.

**권장 조치**:  
- `KafkaRepositoryImpl::new()`에서 SASL 관련 config 설정 여부 명시적 검증 및 로그 출력

---

## 요약

| 번호 | 파일 | 심각도 | 핵심 문제 |
|---|---|---|---|
| 1 | kafka_repository.rs | 🔴 | auto-commit으로 메시지 유실 가능 |
| 2 | es_repository.rs | 🔴 | bulk 부분 실패 시 조용히 Ok 반환 |
| 3 | es_repository.rs | 🔴 | HTTP 평문 통신 |
| 4 | cli_service_impl.rs | 🔴 | 소켓 파일 없을 때 서버 시작 불가 |
| 5 | indexing_service_impl.rs | 🔴 | 영구 재시도 루프 탈출 불가 |
| 6 | es_repository.rs | 🟠 | 전체 데이터 info 로그 (개인정보 + 성능) |
| 7 | es_repository.rs | 🟠 | debug_body 이중 메모리 |
| 8 | batch_service_impl.rs | 🟠 | N+1 ES 쿼리 패턴 |
| 9 | elastic_service_impl.rs | 🟠 | replica 수 하드코딩 |
| 10 | app_config.rs | 🟠 | global() panic 위험 |
| 11 | mysql_service_impl.rs | 🟡 | upsert 트랜잭션 없음 |
| 12 | batch_service_impl.rs | 🟡 | fetch 실패 시 배치 중단 |
| 13 | indexing_service_impl.rs | 🟡 | catch-up 실패 시 인덱스 방치 |
| 14 | cli_service_impl.rs | 🟡 | 소켓 접근 권한 미설정 |
| 15 | public_data_service_impl.rs | 🟢 | API URL 하드코딩 |
| 16 | batch_service_impl.rs | 🟢 | is_weekend 공휴일 미고려 |
| 17 | kafka_repository.rs | 🟢 | SASL 설정 적용 여부 확인 필요 |
