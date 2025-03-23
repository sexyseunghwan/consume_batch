use crate::common::*;

#[doc = "Function to globally initialize the 'CONSUME_DETAIL' variable"]
pub static CONSUME_DETAIL: once_lazy<String> = once_lazy::new(|| {
    dotenv().ok();
    env::var("CONSUME_DETAIL").expect("[ENV file read Error] 'CONSUME_DETAIL' must be set")
});

#[doc = "Function to globally initialize the 'CONSUME_TYPE' variable"]
pub static CONSUME_TYPE: once_lazy<String> = once_lazy::new(|| {
    dotenv().ok();
    env::var("CONSUME_TYPE").expect("[ENV file read Error] 'CONSUME_TYPE' must be set")
});

#[doc = "Function to globally initialize the 'CONSUME_TYPE_SETTINGS' variable"]
pub static CONSUME_TYPE_SETTINGS: once_lazy<String> = once_lazy::new(|| {
    dotenv().ok();
    env::var("CONSUME_TYPE_SETTINGS")
        .expect("[ENV file read Error] 'CONSUME_TYPE_SETTINGS' must be set")
});

#[doc = "Function to globally initialize the 'CONSUME_DETAIL_SETTINGS' variable"]
pub static CONSUME_DETAIL_SETTINGS: once_lazy<String> = once_lazy::new(|| {
    dotenv().ok();
    env::var("CONSUME_DETAIL_SETTINGS")
        .expect("[ENV file read Error] 'CONSUME_DETAIL_SETTINGS' must be set")
});
