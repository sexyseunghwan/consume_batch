//! Spent detail with relations model for Elasticsearch indexing.
//!
//! This module provides the data structure for indexing spent detail information
//! along with related keyword, keyword type, and telegram room data.

use crate::common::*;

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
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, new)]
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
}
