pub use std::{
    cmp,
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    env, fs,
    future::Future,
    io::Write,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
    thread,
    time::Duration,
};

pub use rand::{prelude::SliceRandom, rngs::StdRng, SeedableRng};

pub use tokio::{sync::OnceCell, task};

pub use log::{error, info};

pub use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Logger, Naming, Record};

pub use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc, Weekday,
};
pub use chrono_tz::Asia::Seoul;

pub use serde::{Deserialize, Serialize};

pub use serde_json::{from_value, json, Value};

pub use serde::de::DeserializeOwned;

pub use dotenv::dotenv;

pub use elasticsearch::{
    http::response::Response,
    http::transport::{ConnectionPool, Transport},
    http::transport::{MultiNodeConnectionPool, SingleNodeConnectionPool, TransportBuilder},
    http::Url,
    DeleteByQueryParts, DeleteParts, Elasticsearch, IndexParts, SearchParts,
};

pub use anyhow::{anyhow, Context, Result};

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
    associations::HasTable,
    expression::NonAggregate,
    helper_types::SqlTypeOf,
    mysql::{Mysql, MysqlConnection},
    query_builder::{AsQuery, QueryFragment, QueryId},
    query_dsl::methods::{BoxedDsl, LimitDsl, OrderDsl},
    r2d2::{ConnectionManager, Pool, PooledConnection},
    sql_types::{SingleValue, SqlType},
    AppearsOnTable, Column, Expression, ExpressionMethods, Insertable, JoinOnDsl, QueryDsl,
    Queryable, RunQueryDsl, SelectableExpression, Table,
};

pub use async_trait::async_trait;

//use crate::repository::es_repository::*;
//use crate::repository::kafka_repository::*;
//pub static ELASTICSEARCH_CLIENT: OnceCell<Arc<EsRepositoryPub>> = OnceCell::new();
//pub static KAFKA_PRODUCER: OnceCell<Arc<KafkaRepositoryPub>> = OnceCell::const_new();

pub use regex::Regex;

pub use once_cell::sync::Lazy as once_lazy;

/* Elasticsearch index name to use globally */
pub static CONSUME_DETAIL: &str = "consuming_index_prod_new";
pub static CONSUME_DETAIL_REMOVE: &str = "consuming_index_prod_new_remove";
pub static CONSUME_TYPE: &str = "consuming_index_prod_type_v2";
pub static MEAL_CHECK: &str = "meal_check_index";
