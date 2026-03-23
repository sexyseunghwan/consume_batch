use crate::common::*;

#[doc = "Function responsible for logging"]
pub fn set_global_logger() {
    let log_directory: &str = "logs"; /* Directory to store log files */
    let file_prefix: &str = ""; /* Prefixes for log files */

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
        .format_for_stdout(custom_format)
        .duplicate_to_stdout(Duplicate::Info) /* Duplicate Info-level logs to stdout */
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
