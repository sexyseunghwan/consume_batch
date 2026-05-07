use crate::common::*;

/// Date range used by spend report queries and email rendering.
#[derive(Clone, Copy)]
pub struct ReportDateRange {
    /// First date included in the report range.
    pub start_date: DateTime<Utc>,
    /// Last date included in the report range.
    pub end_date: DateTime<Utc>,
}
