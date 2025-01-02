
mod common;
use common::*;

mod utils_module;
use utils_module::logger_utils::*;

mod repository;

mod models;

//mod schema;

#[tokio::main]
async fn main() {
    
    let set_global_logger = set_global_logger();

    info!("test");
    //println!("Hello, world!");
}
