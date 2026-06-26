use crate::common::*;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};

use super::KafkaRepositoryImpl;

impl KafkaRepositoryImpl {
    pub(super) async fn fetch_committed_offsets_total(
        &self,
        _topic: &str,
        _group_suffix: &str,
    ) -> anyhow::Result<i64> {
        Ok(0)
    }

    /// Returns committed offsets per partition.
    ///
    /// `BaseConsumer::fetch_metadata` and `committed` are blocking calls —
    /// run them in `spawn_blocking` to avoid stalling the tokio worker thread.
    pub(super) async fn fetch_committed_offsets_by_partition(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<HashMap<i32, i64>> {
        // consumer group id 생성
        let group_id: String = format!("{}-{}-{}", self.base_group_id, topic, group_suffix); 

        let brokers: String = self.kafka_brokers.clone();
        let security_protocol: Option<String> = self.security_protocol.clone();
        let sasl_mechanism: Option<String> = self.sasl_mechanism.clone();
        let sasl_username: Option<String> = self.sasl_username.clone();
        let sasl_password: Option<String> = self.sasl_password.clone();
        let group_id_clone: String = group_id.clone();
        let topic_owned: String = topic.to_string();
        /*
            BaseConsumer::fetch_metadata(), committed() 는 동기 블로킹 호출인데 tokio async context 에서 호출하고 있음 -> worker thread를 블로킹시켜 hang 유발 가능.
            spawn_blocking 으로 넘겨줘야 함.

            rdkafka의 BaseConsumer 메서드 중 일부는 blocking 방식.
            예를 들면:   
                fetch_metadata(...)
                committed(...)
            위와 같은 호출은 내부적으로 Kafka broker와 통신하면서 최대 10초까지 기다릴 수 있다.

            async fn 안에서 blocking 작업 실행
                → Tokio worker thread가 막힘
                → 다른 async task들도 영향을 받음
                → 전체 서비스 반응성 저하 가능

            partition_offsets 은 아래와 같이 생겼을 것이다.
            {
                0: 15230,
                1: 14882,
                2: 20104
            }
            ====> 
            {
                partition 0 → committed offset 15230
                partition 1 → committed offset 14882
                partition 2 → committed offset 20104
            }
        */
        let partition_offsets: HashMap<i32, i64> = tokio::task::spawn_blocking(move || {
            
            let mut config: ClientConfig = ClientConfig::new();
            config
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group_id_clone);

            if let (Some(protocol), Some(mechanism), Some(username), Some(password)) = (
                &security_protocol,
                &sasl_mechanism,
                &sasl_username,
                &sasl_password,
            ) {
                config
                    .set("security.protocol", protocol)
                    .set("sasl.mechanism", mechanism)
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            
            let consumer: BaseConsumer = config.create().map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] Failed to create consumer for group '{}': {:?}",
                    group_id_clone, e
                )
            })?;
            
            /*
                토픽의 메타데이터 반환
                - topic 이름
                - partition 목록
                - leader broker
                - replica 정보
                - isr 정보
            */
            let metadata: rdkafka::metadata::Metadata = consumer
                .fetch_metadata(Some(&topic_owned), Duration::from_secs(10))
                .map_err(|e| {
                    anyhow!(
                        "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] Failed to fetch metadata for topic '{}': {:?}",
                        topic_owned, e
                    )
                })?;
            
            let topic_metadata: &rdkafka::metadata::MetadataTopic = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic_owned.as_str())
                .ok_or_else(|| {
                    anyhow!(
                        "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] Topic '{}' not found in metadata",
                        topic_owned
                    )
                })?;

            let mut tpl: TopicPartitionList = TopicPartitionList::new();
            for partition in topic_metadata.partitions() {
                tpl.add_partition(&topic_owned, partition.id());
            }
            
            // 이 consumer는 특정 topic의 특정 partition들을 대상으로 동작해라 뜻 -> 단순히 조회가 목적이기 때문에 구독을 하진 않는다.
            consumer.assign(&tpl).map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] Failed to assign partitions: {:?}",
                    e
                )
            })?;
            
            // 현재 consumer의 group id 기준으로, assign된 partition들의 committed offset을 가져온다.
            let committed_tpl: TopicPartitionList = consumer
                .committed(Duration::from_secs(10))
                .map_err(|e| {
                    anyhow!(
                        "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] Failed to fetch committed offsets for group '{}': {:?}",
                        group_id_clone, e
                    )
                })?;

            if committed_tpl.count() == 0 {
                return Ok(HashMap::new());
            }
            
            let mut offsets: HashMap<i32, i64> = HashMap::new();
            for elem in committed_tpl.elements() {
                let offset: i64 = match elem.offset() {
                    Offset::Offset(o) => o,
                    _ => 0,
                };
                offsets.insert(elem.partition(), offset);
            }

            anyhow::Ok(offsets)
        })
        .await??;

        info!(
            "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] group='{}' topic='{}' partition_offsets={:?}",
            group_id, topic, partition_offsets
        );

        Ok(partition_offsets)
    }
}
