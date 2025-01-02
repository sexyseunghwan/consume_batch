use crate::common::*;

// type MysqlPool = Pool<ConnectionManager<MysqlConnection>>;

// static POOL: once_lazy<Arc<MysqlPool>> = once_lazy::new(|| {
//     dotenv().ok();
//     let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
//     let manager = ConnectionManager::<MysqlConnection>::new(&database_url);
//     Arc::new(Pool::builder().build(manager).expect("Failed to create pool."))
// });

// pub fn get_mysql_pool() -> Arc<MysqlPool> {
//     POOL.clone()
// }