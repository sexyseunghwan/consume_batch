use crate::common::*;

type MysqlPool = Pool<ConnectionManager<MysqlConnection>>;

static POOL: once_lazy<Arc<MysqlPool>> = once_lazy::new(|| {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<MysqlConnection>::new(&database_url);
    Arc::new(
        Pool::builder()
            .build(manager)
            .expect("Failed to create pool."),
    )
});

#[doc = "Functions that return MySQL connection information"]
pub fn get_mysql_pool(
) -> Result<PooledConnection<ConnectionManager<MysqlConnection>>, anyhow::Error> {
    let pool = Arc::clone(&POOL);
    let conn: PooledConnection<ConnectionManager<MysqlConnection>> = pool.get()?;

    let pool_state = pool.state();
    info!("idle_connections: {}", pool_state.idle_connections);

    Ok(conn)
}
