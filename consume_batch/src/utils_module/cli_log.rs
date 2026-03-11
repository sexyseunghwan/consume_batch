use tokio::sync::mpsc;

tokio::task_local! {
    pub static CLI_LOG_TX: Option<mpsc::Sender<String>>;
}
