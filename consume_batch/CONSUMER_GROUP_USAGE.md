# Consumer Group Usage Guide

## 현재 Offset 관리 방식

### 기본 방식 (토픽당 단일 offset)

기본적으로 각 토픽마다 하나의 consumer group을 사용합니다:

```rust
// Group ID: "{base_group_id}-{topic}"
// 예: "my-batch-consumer-spent_detail_topic"

let messages = consume_service
    .consume_messages("spent_detail_topic", 100)
    .await?;
```

**문제점**: 같은 토픽을 여러 작업에서 사용하면 offset이 공유됩니다.

---

## 작업별 독립 Offset 사용 방법

### 방법 1: `consume_messages_with_group` 사용

각 작업마다 고유한 consumer group을 사용하여 독립적인 offset을 관리할 수 있습니다.

```rust
// Group ID: "{base_group_id}-{topic}-{group_suffix}"

// Full indexing 작업 (offset: 0부터 시작)
let full_messages = consume_service
    .consume_messages_with_group("spent_detail_topic", 1000, "full-index")
    .await?;

// Incremental indexing 작업 (offset: 별도 관리)
let incr_messages = consume_service
    .consume_messages_with_group("spent_detail_topic", 100, "incremental")
    .await?;
```

**결과**:
- Full indexing: `my-batch-consumer-spent_detail_topic-full-index`
- Incremental: `my-batch-consumer-spent_detail_topic-incremental`
- 각각 독립적인 offset을 유지!

---

### 방법 2: `consume_messages_as_with_group` 사용 (타입 변환 포함)

메시지를 소비하면서 바로 타입으로 역직렬화:

```rust
// Full indexing 전용 consumer
let full_details: Vec<SpentDetailWithRelations> = consume_service
    .consume_messages_as_with_group(
        "spent_detail_topic",
        1000,
        "full-index"
    )
    .await?;

// Incremental indexing 전용 consumer
let incr_details: Vec<SpentDetailWithRelations> = consume_service
    .consume_messages_as_with_group(
        "spent_detail_topic",
        100,
        "incremental"
    )
    .await?;
```

---

## Consumer Group 구조

### Internal HashMap

내부적으로 `HashMap<String, Arc<StreamConsumer>>`로 관리됩니다:

```
consumers: {
    "spent_detail_topic": Consumer(group: "base-spent_detail_topic"),
    "spent_detail_topic-full-index": Consumer(group: "base-spent_detail_topic-full-index"),
    "spent_detail_topic-incremental": Consumer(group: "base-spent_detail_topic-incremental"),
}
```

각 consumer는 독립적인 offset을 Kafka에 저장합니다.

---

## 실제 사용 시나리오

### Scenario 1: Full Indexing과 Incremental Indexing 분리

```rust
// batch_service_impl.rs

async fn process_spent_detail_full(...) -> anyhow::Result<()> {
    loop {
        // Full indexing은 "full-index" group 사용
        let messages: Vec<SpentDetailWithRelations> = consume_service
            .consume_messages_as_with_group(
                relation_topic,
                batch_size,
                "full-index"  // 👈 독립 offset
            )
            .await?;

        if messages.is_empty() {
            break;
        }

        elastic_service.bulk_index(&new_index_name, messages).await?;
    }
    Ok(())
}

async fn process_spent_detail_incremental(...) -> anyhow::Result<()> {
    loop {
        // Incremental은 "incremental" group 사용
        let messages: Vec<SpentDetailWithRelations> = consume_service
            .consume_messages_as_with_group(
                relation_topic,
                batch_size,
                "incremental"  // 👈 별도의 독립 offset
            )
            .await?;

        if messages.is_empty() {
            break;
        }

        elastic_service.bulk_index(&index_name, messages).await?;
    }
    Ok(())
}
```

---

### Scenario 2: 동일 토픽, 다른 처리 로직

```rust
// 같은 토픽에서 서로 다른 목적으로 소비

// Analytics 팀 - 전체 데이터 분석
let analytics_data = consume_service
    .consume_messages_as_with_group(
        "user_events",
        10000,
        "analytics"
    )
    .await?;

// Real-time 팀 - 실시간 알림
let realtime_data = consume_service
    .consume_messages_as_with_group(
        "user_events",
        100,
        "realtime-alerts"
    )
    .await?;
```

각 팀은 독립적으로 offset을 관리하며, 서로 영향을 주지 않습니다.

---

## API Reference

### KafkaRepository

```rust
trait KafkaRepository {
    // 기본 방식 (토픽당 단일 offset)
    async fn consume_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<Value>, anyhow::Error>;

    // 작업별 독립 offset
    async fn consume_messages_with_group(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,  // 👈 이것으로 구분!
    ) -> Result<Vec<Value>, anyhow::Error>;
}
```

### ConsumeService

```rust
trait ConsumeService {
    // 기본 방식
    async fn consume_messages_as<T: DeserializeOwned>(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<T>, anyhow::Error>;

    // 작업별 독립 offset + 타입 변환
    async fn consume_messages_as_with_group<T: DeserializeOwned>(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,  // 👈 이것으로 구분!
    ) -> Result<Vec<T>, anyhow::Error>;
}
```

---

## 주의사항

1. **Group Suffix 일관성**: 같은 작업은 항상 같은 suffix를 사용해야 offset이 유지됩니다.

2. **Consumer 재사용**: 같은 topic + suffix 조합은 consumer를 재사용합니다 (캐싱).

3. **Offset 초기화**: 새로운 group suffix는 `auto.offset.reset=earliest`로 처음부터 시작합니다.

4. **리소스 관리**: Consumer는 HashMap에 캐싱되므로 너무 많은 suffix를 사용하면 메모리 사용량이 증가할 수 있습니다.

---

## 요약

| 방식 | Group ID 형식 | 사용 케이스 |
|------|--------------|------------|
| 기본 | `base-{topic}` | 단일 작업, 간단한 소비 |
| With Group | `base-{topic}-{suffix}` | 여러 작업, 독립 offset 필요 |

**핵심**: `group_suffix`를 사용하면 같은 토픽에서 여러 작업이 각자의 offset을 유지할 수 있습니다! 🎯
