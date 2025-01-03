use crate::common::*;

use crate::repository::kafka_repository::*;

#[doc = "Function responsible for logging"]
pub fn set_global_logger() {
    let log_directory = "logs"; /* Directory to store log files */
    let file_prefix = ""; /* Prefixes for log files */

    /* Logger setting */
    Logger::try_with_str("info")
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory(log_directory)
                .discriminant(file_prefix),
        )
        .rotate(
            Criterion::Age(Age::Day),  /* daily rotation */
            Naming::Timestamps,        /* Use timestamps for file names */
            Cleanup::KeepLogFiles(10), /* Maintain up to 10 log files */
        )
        .format_for_files(custom_format)
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed: {}", e));
}

#[doc = "Custom Log Format Function"]
fn custom_format(
    w: &mut dyn Write,
    now: &mut flexi_logger::DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(
        w,
        "[{}] [{}] T[{}] {}",
        now.now().format("%Y-%m-%d %H:%M:%S"),
        record.level(),
        std::thread::current().name().unwrap_or("unknown"),
        &record.args()
    )
}

#[doc = "Function that produces messages to kafka"]
async fn logging_kafka(msg: &str) {
    let kafka_producer = get_kafka_producer();
    let msg_owned = msg.to_string();

    let handle = task::spawn_blocking(move || {
        let _kafka_producer_lock = match kafka_producer.lock() {
            Ok(mut kafka_producer_lock) => {
                kafka_producer_lock.produce_message("consume_alert_rust", &msg_owned)
            }
            Err(e) => {
                error!("{:?}", e);
                Ok(())
            }
        };
    });

    match handle.await {
        Ok(_) => (),
        Err(e) => error!("Error waiting for task: {:?}", e),
    }
}

#[doc = "Function that writes the error history to a file and sends it to kafka"]
pub async fn errork(err: anyhow::Error) {
    error!("{:?}", err);
    logging_kafka(&err.to_string()).await;
}

#[doc = "Function that writes the information history to a file and sends it to kafka"]
pub async fn infok(info: &str) {
    info!("{:?}", info);
    logging_kafka(info).await;
}
