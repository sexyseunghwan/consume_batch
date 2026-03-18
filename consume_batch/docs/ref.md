async fn start_socket_server(&self) -> anyhow::Result<()> {        
        let socket_path: &str = AppConfig::global().socket_path();

        // [1] Remove existing socket file to handle unclean shutdowns
        std::fs::remove_file(socket_path)
            .inspect_err(|e| {
                error!("[CliServiceImpl::start_socket_server] {:#}", e);
            })?;
        
        // [2] Create the socket file and bind the listner
        let listener: UnixListener = UnixListener::bind(socket_path)
            .inspect_err(|e| {
                error!("[CliServiceImpl::start_socket_server] Failed to bind socket: {:#}", e);
            })?;
        
        info!(
            "[CliServiceImpl::start_socket_server] CLI socket server listening on {}",
            socket_path
        );
        
        // [3] Start the connection accept loop
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .inspect_err(|e| {
                    error!("[CliServiceImpl::start_socket_server] Failed to accept connection: {:#}", e);
                })?;

            let batch: Arc<B> = Arc::clone(&self.batch_service);
            let config: BatchScheduleConfig = self.schedule_config.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_socket_connection(stream, batch, config).await {
                    error!(
                        "[CliServiceImpl::handle_socket_connection] Connection error: {:#}",
                        e
                    );
                }
            });
        }
    }