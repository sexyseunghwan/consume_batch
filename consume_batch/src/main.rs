/*
Author      :   Seunghwan Shin
Create date :   2025-01-01
Description :

History     :   2025-01-01 Seunghwan Shin       # [v.1.0.0] first create.
                2025-03-22 Seunghwan Shin       # [v.1.1.0] Change the RDB-related crate (Diesel -> Sear-orm)
                2025-05-28 Seunghwan Shin       # [v.1.1.1] Correct duplicate index problem.
                2025-06-09 Seunghwan Shin       # [v.1.1.2] Unindexable issues exist when duplicate documents exist.
                2026-03-05 Seunghwan Shin       # [v.2.0.0] 1) Refactor code architecture.
                                                            2) Add CLI mode and scheduler mode support.
                                                            3) Improve code structure by seperation responsibilities.
                2026-04-02 Seunghwan Shin       # [v.2.1.0] Updated the indexing structure and added dependency injection for the service.
                2026-04-20 Seunghwan Shin       # [v.3.0.0] 1) Add doc comments to all functions.
                                                            2) Rename UserPaymentMethod -> UserPaymentMethods.
                                                            3) Change money-related fields from i32 to i64.
                                                            4) Fix bulk_index to support explicit doc_id_field (Option<&str>).
                                                            5) Add consecutive error limit to incremental indexing loops.
                                                            6) Add orphaned index cleanup on full indexing pipeline failure.
                                                            7) Add exponential backoff retry to fetch_spent_details.
                                                            8) Add transaction to upsert_spent_detail_indexing.
                2026-04-27 Seunghwan Shin       # [v.3.1.0] Apply unified function naming conventions across all modules.
                                                            - initialize : initialization functions (init, load, from_env, create_*)
                                                            - get / set  : property accessor functions
                                                            - find       : data query functions (get_*, fetch_*, consume_*, read_*)
                                                            - input      : data insert functions (insert, bulk_index, send_message, produce_*)
                                                            - modify     : data update functions (update_*, bulk_update, swap_alias, upsert_*)
                                                            - delete     : data delete functions (bulk_delete, purge_topic, cleanup_*)
                                                            - is         : boolean return functions (needs_upsert -> is_upsert_required)
                                                            - to         : type conversion functions (convert_* -> to_*)
                                                            - By         : preposition for "do A based on B" (process_batch -> input_batch_by_schedule)
                2026-04-29 Seunghwan Shin       # [v.3.2.0] 1) Add modify_all_spent_detail_indexing_type to update consume_keyword_type_id and consume_keyword_type in SPENT_DETAIL_INDEXING.
                                                            2) Add modify_spent_detail_indexing_type_batch to MysqlService trait and impl.
                                                            3) Wrap both spent_detail type update calls into modify_all_spent_detail_types.
                2026-05-04 Seunghwan Shin       # [v.3.3.0] 1) Add SmtpService trait and SmtpServiceImpl using lettre crate.
                                                            2) Add monthly_spent_report batch job: sends per-user HTML spend summary on the 1st of each month.
                                                            3) Add find_users_monthly_spent_summary to MysqlService (GROUP BY on SPENT_DETAIL_INDEXING).
                                                            4) Add SMTP_HOST / SMTP_ID / SMTP_PW env vars to AppConfig.
                2026-05-11 Seunghwan Shin       # [v.3.4.0] 1) Implement process_agg_group: HTML report generation and SMTP delivery per agg_group_seq.
                                                            2) Add send_weekly_spent_report: weekly spend report with 7-day rolling date range.
                                                            3) Add category summary table (spend by consume_keyword_type with percentage share).
                                                            4) Add period-over-period comparison: current vs previous period total, change amount and rate.
                                                            5) Add build_period_summary_html: two-period comparison table with colour-coded change column.
                                                            6) Add prev_date_range to both monthly and weekly report flows for comparison queries.
                                                            7) Generalise build_report_html with {{REPORT_TITLE}}, {{COMPARISON}}, {{PERIOD_SUMMARY}} placeholders.
*/

mod common;
use common::*;

mod utils_module;

mod controller;
use controller::cli_client_controller::CliClientController;
use controller::main_controller::*;

mod entity;

mod service;
use service::{
    batch_service_impl::*, redis_service_impl::*, cli_service_impl::CliServiceImpl,
    consume_service_impl::*, elastic_service_impl::*, indexing_service_impl::IndexingServiceImpl,
    mysql_service_impl::*, producer_service_impl::*,
    public_data_service_impl::PublicDataServiceImpl, smtp_service_impl::SmtpServiceImpl,
};

mod service_trait;

mod repository;
use repository::{es_repository::*, kafka_repository::*, mysql_repository::*, redis_repository::*};

mod models;

mod app_config;
use app_config::AppConfig;

mod global_state;

mod config;

mod enums;

mod dtos;

mod api;

// Type aliases
type RedisService = RedisServiceImpl<RedisRepositoryImpl>;
type ElasticService = ElasticServiceImpl<EsRepositoryImpl>;
type MysqlService = MysqlServiceImpl<MysqlRepositoryImpl>;
type ConsumeService = ConsumeServiceImpl<KafkaRepositoryImpl>;
type ProducerService = ProducerServiceImpl<KafkaRepositoryImpl>;
type PublicDataSvc = PublicDataServiceImpl;
type SmtpSvc = SmtpServiceImpl;
type IndexingSvc =
    IndexingServiceImpl<MysqlService, ProducerService, ElasticService, ConsumeService>;
type BatchSvc = BatchServiceImpl<
    MysqlService,
    ElasticService,
    ConsumeService,
    ProducerService,
    PublicDataSvc,
    IndexingSvc,
    SmtpSvc,
    RedisService,
>;
type CliSvc = CliServiceImpl<BatchSvc>;
type Controller = MainController<BatchSvc, CliSvc>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let args: Vec<String> = std::env::args().collect();

    // Option to run the application in CLI mode.
    // Check before AppConfig::initialize() so CLI client does not require all service env vars.
    if args
        .get(1)
        .map(|s| s == "--cli" || s == "-cli")
        .unwrap_or(false)
    {
        let socket_path: String = std::env::var("SOCKET_PATH")
            .unwrap_or_else(|_| "./socket/consume_batch.sock".to_string());
        CliClientController::run(&socket_path).await;
        return Ok(());
    }

    AppConfig::initialize().map_err(|e| anyhow!("Failed to initialize AppConfig: {}", e))?;
    set_global_logger();

    info!("Indexing Batch Program Start.");

    let elastic_repo: EsRepositoryImpl =
        EsRepositoryImpl::new().inspect_err(|e| error!("[main] elastic_repo: {:#}", e))?;

    let mysql_repo: MysqlRepositoryImpl = MysqlRepositoryImpl::new()
        .await
        .inspect_err(|e| error!("[main] mysql_repo: {:#}", e))?;

    let kafka_repo: KafkaRepositoryImpl =
        KafkaRepositoryImpl::new().inspect_err(|e| error!("[main] kafka_repo: {:#}", e))?;

    let redis_repo: RedisRepositoryImpl = RedisRepositoryImpl::new()
        .await
        .inspect_err(|e| error!("[main] redis_repo: {:#}", e))?;

    let shared_kafka_repo: Arc<KafkaRepositoryImpl> = Arc::new(kafka_repo);

    // Initialize services with dependency injection
    // Wrap in Arc first so they can be shared between BatchServiceImpl and IndexingServiceImpl
    let elastic_query_service: Arc<ElasticService> =
        Arc::new(ElasticServiceImpl::new(elastic_repo));
    let mysql_query_service: Arc<MysqlService> = Arc::new(MysqlServiceImpl::new(mysql_repo));
    let redis_service: RedisService = RedisServiceImpl::new(redis_repo);

    // Share Kafka repository across multiple services (clone is cheap - only Arc increment)
    let consume_service: Arc<ConsumeService> =
        Arc::new(ConsumeServiceImpl::new(Arc::clone(&shared_kafka_repo)));
    let producer_service: Arc<ProducerService> =
        Arc::new(ProducerServiceImpl::new(Arc::clone(&shared_kafka_repo)));

    // public DATA... TO..DO...
    let public_data_api_key: String = AppConfig::get_global()?
        .public_data_api_key()
        .clone()
        .unwrap_or_default();
    let public_data_service: PublicDataSvc = PublicDataServiceImpl::new(public_data_api_key);

    let smtp_service: SmtpSvc = {
        let cfg = AppConfig::get_global()?;
        SmtpServiceImpl::new(
            cfg.smtp_host().clone().unwrap_or_default(),
            cfg.smtp_id().clone().unwrap_or_default(),
            cfg.smtp_pw().clone().unwrap_or_default(),
        )
    };
    
    // Create indexing service — shares the same service Arc refs as BatchServiceImpl
    let indexing_service: IndexingSvc = IndexingServiceImpl::new(
        Arc::clone(&mysql_query_service),
        Arc::clone(&producer_service),
        Arc::clone(&elastic_query_service),
        Arc::clone(&consume_service),
    );
    
    // Create batch service with all dependencies
    let batch_service: BatchSvc = BatchServiceImpl::new(
        mysql_query_service,
        elastic_query_service,
        consume_service,
        producer_service,
        public_data_service,
        indexing_service,
        smtp_service,
        redis_service,
    )
    .inspect_err(|e| error!("[main] batch_service: {:#}", e))?;
    
    // Create CLI service: shares the batch service via Arc for on-demand execution
    let cli_service: CliSvc = CliServiceImpl::new(
        Arc::new(batch_service.clone()),
        batch_service.schedule_config().clone(),
    );

    // Create main controller with both services
    let main_controller: Controller = MainController::new(batch_service, Arc::new(cli_service));

    main_controller
        .main_task()
        .await
        .inspect_err(|e| error!("[main] {:#}", e))?;

    Ok(())
}
