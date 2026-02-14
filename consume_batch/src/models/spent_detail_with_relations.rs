//! Spent detail with relations model for Elasticsearch indexing.
//!
//! This module provides the data structure for indexing spent detail information
//! along with related keyword, keyword type, and telegram room data.

use crate::common::*;

use crate::enums::IndexingType;

use std::str::FromStr;

/// Internal structure for FromQueryResult that stores indexing_type as String
#[derive(Debug, Clone, FromQueryResult)]
pub struct SpentDetailWithRelationsRaw {
    spent_idx: i64,
    spent_name: String,
    spent_money: i32,
    spent_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    user_seq: i64,
    consume_keyword_type_id: i64,
    consume_keyword_type: String,
    room_seq: i64,
    indexing_type: String,
}

/// Represents a spent detail with all related information for indexing.
///
/// This structure combines information from SPENT_DETAIL, COMMON_CONSUME_PRODT_KEYWORD,
/// COMMON_CONSUME_KEYWORD_TYPE, USERS, and TELEGRAM_ROOM tables.
///
/// # Fields
///
/// * `spent_idx` - Primary key of the spent detail
/// * `spent_name` - Name/description of the spending
/// * `spent_money` - Amount spent
/// * `spent_at` - Date and time of the spending
/// * `created_at` - Record creation timestamp
/// * `user_seq` - User identifier
/// * `consume_keyword_id` - Keyword identifier
/// * `consume_keyword` - The actual keyword text
/// * `consume_keyword_type_id` - Keyword type identifier
/// * `consume_keyword_type` - The type/category of the keyword
/// * `room_seq` - Telegram room identifier
#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct SpentDetailWithRelations {
    /// Primary key of the spent detail
    pub spent_idx: i64,

    /// Name/description of the spending
    pub spent_name: String,

    /// Amount spent
    pub spent_money: i32,

    /// Date and time of the spending
    pub spent_at: DateTime<Utc>,

    /// Record creation timestamp
    pub created_at: DateTime<Utc>,

    /// User identifier
    pub user_seq: i64,

    /// Keyword type identifier
    pub consume_keyword_type_id: i64,

    /// The type/category of the keyword
    pub consume_keyword_type: String,

    /// Telegram room identifier
    pub room_seq: i64,

    /// Indexing Type - I,U,D
    pub indexing_type: IndexingType,
}


/// Elasticsearch-specific version of SpentDetailWithRelations.
///
/// This structure excludes the `indexing_type` field which is used for
/// processing logic but should not be indexed in Elasticsearch.
#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct SpentDetailWithRelationsEs {
    /// Primary key of the spent detail
    pub spent_idx: i64,

    /// Name/description of the spending
    pub spent_name: String,

    /// Amount spent
    pub spent_money: i32,

    /// Date and time of the spending
    pub spent_at: DateTime<Utc>,

    /// Record creation timestamp
    pub created_at: DateTime<Utc>,

    /// User identifier
    pub user_seq: i64,

    /// Keyword type identifier
    pub consume_keyword_type_id: i64,

    /// The type/category of the keyword
    pub consume_keyword_type: String,

    /// Telegram room identifier
    pub room_seq: i64,
}

impl From<SpentDetailWithRelations> for SpentDetailWithRelationsEs {
    fn from(src: SpentDetailWithRelations) -> Self {
        Self {
            spent_idx: src.spent_idx,
            spent_name: src.spent_name,
            spent_money: src.spent_money,
            spent_at: src.spent_at,
            created_at: src.created_at,
            user_seq: src.user_seq,
            consume_keyword_type_id: src.consume_keyword_type_id,
            consume_keyword_type: src.consume_keyword_type,
            room_seq: src.room_seq,
        }
    }
}

impl From<SpentDetailWithRelationsRaw> for SpentDetailWithRelations {
    fn from(raw: SpentDetailWithRelationsRaw) -> Self {
        Self {
            spent_idx: raw.spent_idx,
            spent_name: raw.spent_name,
            spent_money: raw.spent_money,
            spent_at: raw.spent_at,
            created_at: raw.created_at,
            user_seq: raw.user_seq,
            consume_keyword_type_id: raw.consume_keyword_type_id,
            consume_keyword_type: raw.consume_keyword_type,
            room_seq: raw.room_seq,
            indexing_type: IndexingType::from_str(&raw.indexing_type)
                .unwrap_or(IndexingType::Insert),
        }
    }
}

impl FromQueryResult for SpentDetailWithRelations {
    fn from_query_result(res: &QueryResult, pre: &str) -> Result<Self, DbErr> {
        let raw: SpentDetailWithRelationsRaw =
            SpentDetailWithRelationsRaw::from_query_result(res, pre)?;
        Ok(raw.into())
    }
}
