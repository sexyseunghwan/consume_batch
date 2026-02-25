//! CLI client controller.
//!
//! Manages the interactive CLI client that connects to a running service instance
//! via a Unix domain socket.
//!
//! # Architecture
//!
//! ```text
//! CliClientController::run()
//!      │
//!      ├── Connect to Unix socket (AppConfig::socket_path)
//!      │
//!      └── Loop:
//!           ├── read_until_prompt()  → display server menu/response
//!           ├── read_user_input()    → read from stdin
//!           ├── send input to server
//!           └── if "0" or "q" → read final message and exit
//! ```

use crate::app_config::AppConfig;
use crate::common::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

pub struct CliClientController;

impl CliClientController {
    /// Runs the interactive CLI client that connects to a running service instance.
    ///
    /// Connects to the service via a Unix domain socket and provides a menu-driven
    /// interface for triggering batch jobs on demand. The server sends a numbered
    /// menu; the client reads user input and forwards it back to the server for execution.
    ///
    /// # Errors
    ///
    /// Prints to stderr and exits the loop cleanly if:
    /// - The service socket is not available (service not running)
    /// - A read or write on the socket fails mid-session
    pub async fn run() {
        info!("Indexing Batch Program Start. [CLI mode]");

        let socket_path: &str = AppConfig::global().socket_path();
        let stream: UnixStream = match UnixStream::connect(socket_path).await {
            Ok(stream) => stream,
            Err(_) => {
                eprintln!(
                    "[ERROR] Unable to connect to the service. ({})",
                    socket_path
                );
                eprintln!("The service must be running before connecting.");
                return;
            }
        };

        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader: tokio::io::BufReader<tokio::io::ReadHalf<UnixStream>> =
            tokio::io::BufReader::new(read_half);
        let mut writer: tokio::io::WriteHalf<UnixStream> = write_half;

        loop {
            if let Err(e) = Self::read_until_prompt(&mut reader).await {
                eprintln!("[ERROR] Failed to read from server: {}", e);
                break;
            }

            let user_input: String = match Self::read_user_input().await {
                Some(input) => input,
                None => break,
            };

            if let Err(e) = writer.write_all(user_input.as_bytes()).await {
                eprintln!("[ERROR] Failed to send to server: {}", e);
                break;
            }

            if Self::is_exit_command(&user_input) {
                let _ = Self::read_until_prompt(&mut reader).await;
                break;
            }
        }
    }

    /// Reads and prints server messages line by line until an input prompt is detected.
    ///
    /// Stops when a line ending with `"Input:"` is found, which signals that the
    /// server is waiting for user input.
    async fn read_until_prompt(
        reader: &mut tokio::io::BufReader<tokio::io::ReadHalf<UnixStream>>,
    ) -> std::io::Result<()> {
        let mut buffer: String = String::new();

        loop {
            buffer.clear();
            let bytes_read: usize = reader.read_line(&mut buffer).await?;

            if bytes_read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Server closed connection",
                ));
            }

            print!("{}", buffer);

            if let Err(e) = std::io::stdout().flush() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to flush stdout: {}", e),
                ));
            }

            if buffer.trim_end().ends_with("Input:") {
                break;
            }
        }

        Ok(())
    }

    /// Reads a single line of input from stdin without blocking the async runtime.
    ///
    /// Returns `Some(String)` with the user's input, or `None` on EOF or error.
    async fn read_user_input() -> Option<String> {
        tokio::task::spawn_blocking(|| {
            let mut line: String = String::new();
            match std::io::stdin().read_line(&mut line) {
                Ok(0) | Err(_) => None,
                Ok(_) => Some(line),
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Returns `true` if the input is `"0"` or `"q"` (case-insensitive).
    fn is_exit_command(input: &str) -> bool {
        let trimmed: &str = input.trim();
        trimmed == "0" || trimmed.eq_ignore_ascii_case("q")
    }
}
