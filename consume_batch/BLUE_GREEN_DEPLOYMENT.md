# Blue/Green Deployment Strategy

## 개요

이 문서는 Elasticsearch 인덱스에 대한 Blue/Green 배포 전략을 설명합니다. 풀색인(Full Indexing) 중에도 증분색인(Incremental Indexing)이 안전하게 동작하도록 Read/Write Alias를 분리하여 무중단 배포를 구현합니다.

## 핵심 개념

### 1. Read/Write Alias 분리

```
┌─────────────────────────────────────────────────────────────┐
│                    Alias Architecture                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  read_spent_detail_dev  ──┐                                 │
│                           ├──► spent_detail_dev_20260214... │
│  write_spent_detail_dev ──┘    (Blue Index - 현재 운영)     │
│                                                             │
│                                                             │
│  풀색인 중:                                                  │
│  read_spent_detail_dev  ──┐                                 │
│                           ├──► spent_detail_dev_20260215... │
│  write_spent_detail_dev ──┘    (Green Index - 신규)         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

- **`read_{alias}`**: 검색 트래픽이 사용하는 alias
  - 풀색인 완료 후 새로운 인덱스로 스왑
  - Traffic weight를 통한 점진적 전환 지원

- **`write_{alias}`**: 증분 업데이트가 사용하는 alias
  - 풀색인 중에는 기존 인덱스(Blue)를 계속 가리킴
  - 검증 완료 후 수동으로 새 인덱스(Green)로 전환

### 2. Kafka Topic 분리

- **`full_spent_detail_dev`**: 풀색인용 토픽
  - MySQL 전체 데이터를 주기적으로 프로듀싱
  - 풀색인 작업에서만 소비

- **`spent_detail_dev`**: 증분색인용 토픽
  - 실시간 변경사항을 프로듀싱
  - 증분색인 작업에서 지속적으로 소비
  - 풀색인 시 catch-up을 위해서도 소비

## 배포 프로세스

### Phase 1: 초기 상태 (Blue)

```
MySQL ──► full_spent_detail_dev (토픽)
     └──► spent_detail_dev (토픽)

read_spent_detail_dev  ──┐
                         ├──► spent_detail_dev_v1 (Blue)
write_spent_detail_dev ──┘
```

- 기존 인덱스 `spent_detail_dev_v1`이 운영 중
- Read/Write alias 모두 Blue를 가리킴
- 증분색인이 `write_` alias로 지속적으로 업데이트

### Phase 2: 풀색인 시작 (Blue + Green)

```
풀색인 작업:
1. spent_detail_dev_20260214120000 (Green) 생성
2. full_spent_detail_dev 토픽에서 전체 데이터 색인
3. spent_detail_dev 토픽에서 catch-up (변경사항 반영)

동시에 진행:
- 증분색인은 계속 write_spent_detail_dev (Blue)로 업데이트
- 검색은 read_spent_detail_dev (Blue)에서 처리
```

**핵심**: 풀색인과 증분색인이 서로 다른 인덱스에 동작하므로 충돌 없음

### Phase 3: Read Alias 전환 (Green으로 트래픽 이동)

```
read_spent_detail_dev  ──┬──► spent_detail_dev_v1 (Blue)
                         └──► spent_detail_dev_20260214120000 (Green) ✓

write_spent_detail_dev ───► spent_detail_dev_v1 (Blue)
```

- `read_` alias만 Green으로 전환
- `traffic_weight` 설정으로 점진적 전환 가능
  - `0.5`: 50% 트래픽만 Green으로
  - `1.0`: 100% 트래픽 Green으로
- `write_` alias는 여전히 Blue를 가리킴
- **증분색인은 여전히 Blue에 기록됨**

### Phase 4: 검증 및 Write Alias 전환

```
검증 항목:
✓ 검색 결과 비교 (Blue vs Green)
✓ Latency 비교
✓ Error rate 비교

검증 완료 후:
read_spent_detail_dev  ──┐
                         ├──► spent_detail_dev_20260214120000 (Green) ✓
write_spent_detail_dev ──┘

증분색인도 이제 Green으로 업데이트
```

### Phase 5: Blue 인덱스 삭제

```
모니터링 기간 후:
- Blue 인덱스 삭제
- Green이 새로운 Blue가 됨
```

## 구현 세부사항

### 1. BatchScheduleItem 확장

```rust
#[derive(Debug, Deserialize, Clone, Getters)]
pub struct BatchScheduleItem {
    // ... 기존 필드들

    /// Traffic weight for Blue/Green deployment (0.0 ~ 1.0)
    #[serde(default)]
    traffic_weight: f32,
}
```

### 2. ElasticService 메서드 추가

```rust
#[async_trait]
pub trait ElasticService {
    // 기존 메서드들...

    /// Write alias 업데이트 (단일 인덱스로만 지정)
    async fn update_write_alias(&self, write_alias: &str, target_index: &str) -> anyhow::Result<()>;

    /// Read alias 업데이트 (traffic weight 지원)
    async fn update_read_alias_with_weight(
        &self,
        read_alias: &str,
        new_index: &str,
        traffic_weight: f32,
    ) -> anyhow::Result<()>;
}
```

### 3. 풀색인 프로세스

```rust
async fn process_spent_detail_full(...) -> anyhow::Result<()> {
    // 1. Green 인덱스 생성
    let new_index = prepare_full_index(...).await?;

    // 2. 전체 데이터 색인 (full_spent_detail_dev 토픽)
    let static_indexed = process_spent_detail_static(..., &new_index).await?;

    // 3. 증분 변경사항 catch-up (spent_detail_dev 토픽)
    let dynamic_indexed = process_spent_detail_dynamic(..., &new_index).await?;

    // 4. 운영 설정 적용
    elastic_service.update_index_settings(&new_index, &production_settings).await?;

    // 5. READ alias만 업데이트 (write alias는 유지)
    elastic_service
        .update_read_alias_with_weight(&read_alias, &new_index, traffic_weight)
        .await?;

    // Write alias는 수동 전환 필요
}
```

### 4. 증분색인 프로세스

```rust
async fn process_spent_detail_incremental(...) -> anyhow::Result<()> {
    let write_alias = format!("write_{}", index_alias);

    loop {
        let messages = consume_service
            .consume_messages_as_with_group(relation_topic, batch_size, consumer_group)
            .await?;

        // write_alias로 색인 (Blue 또는 Green, 설정에 따라)
        elastic_service.bulk_index(&write_alias, es_messages).await?;

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

## 설정 예시

### batch_schedule.toml

```toml
[[batch_schedule]]
batch_name = "spent_detail_full"
index_name = "spent_detail_dev"
enabled = true
relation_topic = "full_spent_detail_dev"      # 풀색인 토픽
relation_topic_sub = "spent_detail_dev"       # 증분색인 토픽 (catch-up용)
consumer_group = "full_spent_detail_group"
batch_size = 100
cron_schedule_apply = false
cron_schedule = "0 5,10,15,20,25,30,35,40,45,50,55 1-23 * * *"
mapping_schema = "./src/config/elastic_schema/consume_index_prod_type.json"
traffic_weight = 1.0  # 0.0~1.0: 새 인덱스로 보낼 트래픽 비율

[[batch_schedule]]
batch_name = "spent_detail_incremental"
index_name = "spent_detail_dev"
enabled = true
relation_topic = "spent_detail_dev"           # 증분색인 토픽
relation_topic_sub = ""
consumer_group = "incremental_spent_detail_group"
batch_size = 100
cron_schedule_apply = true
cron_schedule = "* * * * * *"  # 지속적으로 실행
mapping_schema = "./src/config/elastic_schema/consume_index_prod_type.json"
traffic_weight = 0.0  # 증분색인에서는 미사용
```

## 장점

### 1. 무중단 배포
- 풀색인 중에도 검색/쓰기 가능
- 트래픽 점진적 전환으로 리스크 최소화

### 2. 데이터 일관성
- 풀색인 중 발생한 변경사항도 catch-up으로 반영
- 증분색인과 풀색인 간 충돌 없음

### 3. 롤백 용이
- Read alias만 다시 Blue로 전환
- 데이터 손실 없음

### 4. 점진적 검증
- `traffic_weight`로 일부 트래픽만 전환
- A/B 테스트 및 성능 비교 가능

## 모니터링 포인트

### 풀색인 중
- [ ] 풀색인 진행률 (full_spent_detail_dev 토픽 소비)
- [ ] Catch-up 진행률 (spent_detail_dev 토픽 소비)
- [ ] Green 인덱스 document 수
- [ ] Blue 인덱스 document 수 (증분색인으로 계속 증가)

### Read Alias 전환 후
- [ ] 검색 결과 비교 (Blue vs Green)
- [ ] 검색 latency 비교
- [ ] Error rate 비교
- [ ] Document 수 차이

### Write Alias 전환 후
- [ ] 증분색인 에러 없음 확인
- [ ] Green 인덱스 document 수 증가 확인
- [ ] Blue 인덱스 더이상 업데이트 안됨 확인

## 수동 작업

### Write Alias 전환 (검증 완료 후)

```bash
# Elasticsearch API를 통한 수동 전환
POST /_aliases
{
  "actions": [
    {
      "remove": {
        "index": "spent_detail_dev_v1",
        "alias": "write_spent_detail_dev"
      }
    },
    {
      "add": {
        "index": "spent_detail_dev_20260214120000",
        "alias": "write_spent_detail_dev"
      }
    }
  ]
}
```

### Blue 인덱스 삭제 (모니터링 완료 후)

```bash
DELETE /spent_detail_dev_v1
```

## 트러블슈팅

### Q: 풀색인 후 document 수가 다른 경우
**A**: Catch-up 시점의 차이일 수 있음. 증분색인 토픽을 완전히 소비했는지 확인.

### Q: 증분색인이 동작하지 않는 경우
**A**: `write_` alias가 올바른 인덱스를 가리키는지 확인.

### Q: Traffic weight가 동작하지 않는 경우
**A**: 현재 구현은 0.5 기준으로 on/off만 지원. 실제 weighted routing은 추후 구현 필요.

## 다음 단계

- [ ] Weighted routing 실제 구현 (Elasticsearch routing 기능 활용)
- [ ] 자동 Write alias 전환 로직 (검증 메트릭 기반)
- [ ] Blue 인덱스 자동 삭제 (설정된 기간 후)
- [ ] 모니터링 대시보드 구축
