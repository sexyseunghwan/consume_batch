use crate::common::*;

/// Represents an aggregation group for grouping Telegram rooms.
///
/// Maps to the `AGG_GROUP` table. One `AggGroup` can be associated with
/// multiple `TELEGRAM_ROOM` rows (1:N relationship).
///
/// # Fields
///
/// * `agg_group_seq`  - Primary key (auto-increment)
/// * `agg_group_name` - Display name of the aggregation group
/// * `is_active`      - Whether the group is currently active
/// * `created_at`     - Record creation timestamp
/// * `updated_at`     - Record last-update timestamp (nullable)
/// * `created_by`     - Who created this record
/// * `updated_by`     - Who last updated this record (nullable)
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct AggGroup {
    /// Primary key of the aggregation group
    pub agg_group_seq: i64,

    /// Display name of the aggregation group
    pub agg_group_name: String,

    /// Whether the group is currently active
    pub is_active: bool,

    /// Record creation timestamp
    pub created_at: DateTime<Utc>,

    /// Record last-update timestamp
    pub updated_at: Option<DateTime<Utc>>,

    /// Who created this record
    pub created_by: String,

    /// Who last updated this record
    pub updated_by: Option<String>,
}
