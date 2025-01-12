use crate::common::*;

#[doc = "Kafka connection object to be used in a single tone"]
static KAFKA_PRODUCER: once_lazy<Arc<Mutex<KafkaRepositoryPub>>> =
    once_lazy::new(|| initialize_kafka_clients());

#[doc = "Function to initialize Kafka connection instances"]
pub fn initialize_kafka_clients() -> Arc<Mutex<KafkaRepositoryPub>> {
    let kafka_host: String = env::var("KAFKA_HOST")
        .expect("[ENV file read Error][initialize_db_clients()] 'KAFKA_HOST' must be set");
    let kafka_host_vec: Vec<String> = kafka_host.split(',').map(|s| s.to_string()).collect();

    let produce_broker: Producer = match Producer::from_hosts(kafka_host_vec.to_owned())
        .with_ack_timeout(Duration::from_secs(3)) /* Timeout settings for message transfer confirmation */ 
        .with_required_acks(RequiredAcks::One)/* If the message transfer is delivered to at least one broker, it is considered a success */ 
        .create() {
            Ok(kafka_producer) => kafka_producer,
            Err(e) => {
                error!("{:?}", e);
                panic!("{:?}", e)
            }
        };

    let kafka_producer = KafkaRepositoryPub::new(produce_broker);
    Arc::new(Mutex::new(kafka_producer))
}

#[doc = "Function that returns Kafka producer singleton"]
pub fn get_kafka_producer() -> Arc<Mutex<KafkaRepositoryPub>> {
    Arc::clone(&KAFKA_PRODUCER)
}

#[async_trait]
pub trait KafkaRepository {
    fn produce_message(&mut self, topic: &str, message: &str) -> Result<(), anyhow::Error>;
}

#[derive(new)]
pub struct KafkaRepositoryPub {
    produce_broker: Producer,
}

#[async_trait]
impl KafkaRepository for KafkaRepositoryPub {
    #[doc = "Function that send message to Kafka"]
    /// # Arguments
    /// * `topic` - kafka topic to produce
    /// * `message` - a message to produce
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    fn produce_message(&mut self, topic: &str, message: &str) -> Result<(), anyhow::Error> {
        let produce_broker = &mut self.produce_broker;

        let _result = produce_broker.send(&KafkaRecord::from_value(topic, message))?;

        Ok(())
    }
}
