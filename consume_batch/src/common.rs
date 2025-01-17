pub use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
    env,
    fs::File,
    future::Future,
    io::{BufReader, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

pub use rand::{prelude::SliceRandom, rngs::StdRng, SeedableRng};

pub use tokio::task;

pub use log::{error, info};

pub use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Logger, Naming, Record};

pub use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc,
};
pub use chrono_tz::Asia::Seoul;

pub use serde::{Deserialize, Serialize};

pub use serde_json::{json, Value};



pub use http::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

pub use dotenv::dotenv;

pub use elasticsearch::{
    http::response::Response,
    http::transport::{ConnectionPool, Transport},
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    http::Url,
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetAliasParts, IndicesRefreshParts},
    BulkOperation, BulkParts, DeleteParts, Elasticsearch, IndexParts,
    SearchParts,
};

pub use anyhow::{anyhow, Result};

pub use derive_new::new;
pub use getset::Getters;

pub use num_format::{Locale, ToFormattedString};

// pub use rdkafka:: {
//     config::ClientConfig,
//     consumer::Consumer,
//     producer::{FutureProducer, FutureRecord},
//     message::Message as KafkaMessage
// };

// pub use kafka::{
//     producer::{Producer, Record as KafkaRecord, RequiredAcks}
// };

pub use kafka::producer::{Producer, Record as KafkaRecord, RequiredAcks};

pub use diesel::{
    dsl::count_star,
    mysql::MysqlConnection,
    r2d2::{ConnectionManager, Pool, PooledConnection}, AsChangeset, ExpressionMethods, Insertable,
    NullableExpressionMethods, QueryDsl, Queryable, QueryableByName, RunQueryDsl,
};

pub use async_trait::async_trait;

//use crate::repository::es_repository::*;
//use crate::repository::kafka_repository::*;
//pub static ELASTICSEARCH_CLIENT: OnceCell<Arc<EsRepositoryPub>> = OnceCell::new();
//pub static KAFKA_PRODUCER: OnceCell<Arc<KafkaRepositoryPub>> = OnceCell::const_new();

pub use regex::Regex;

pub use once_cell::sync::Lazy as once_lazy;

pub use strsim::levenshtein;

/* Elasticsearch index name to use globally */
pub static CONSUME_DETAIL: &str = "consuming_index_prod_new_v10";
pub static CONSUME_DETAIL_REMOVE: &str = "consuming_index_prod_new_remove";
pub static CONSUME_TYPE: &str = "consuming_index_prod_type_v10";
pub static MEAL_CHECK: &str = "meal_check_index";

pub static CONSUME_TYPE_SETTINGS: &str = "./data/consume_index_prod_type.json";
pub static CONSUME_DETAIL_SETTINGS: &str = "./data/consuming_index_prod_new.json";
