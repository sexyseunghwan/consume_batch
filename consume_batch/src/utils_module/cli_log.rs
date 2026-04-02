use tokio::sync::mpsc;

tokio::task_local! {
    pub static CLI_LOG_TX: Option<mpsc::Sender<String>>;
}

/// Logs a message via the standard `log` crate and optionally forwards it to the CLI log channel.
///
/// Supports `info`, `warn`, and `error` levels.
#[macro_export]
macro_rules! batch_log {
    (info, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        log::info!("{}", msg);
        let _ = $crate::utils_module::cli_log::CLI_LOG_TX.try_with(|opt| {
            if let Some(tx) = opt {
                let _ = tx.try_send(msg);
            }
        });
    }};
    (error, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        log::error!("{}", msg);
        let _ = $crate::utils_module::cli_log::CLI_LOG_TX.try_with(|opt| {
            if let Some(tx) = opt {
                let _ = tx.try_send(format!("[ERROR] {}", msg));
            }
        });
    }};
    (warn, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        log::warn!("{}", msg);
        let _ = $crate::utils_module::cli_log::CLI_LOG_TX.try_with(|opt| {
            if let Some(tx) = opt {
                let _ = tx.try_send(format!("[WARN] {}", msg));
            }
        });
    }};
}
