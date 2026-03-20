use std::collections::HashSet;

use async_trait::async_trait;
use chrono::NaiveDate;

/// Trait for fetching data from the Korean public data portal (data.go.kr).
#[async_trait]
pub trait PublicDataService: Send + Sync {
    /// Fetches Korean legal holidays for every year in `start_year..=end_year`.
    ///
    /// Returns a `HashSet<NaiveDate>` containing only dates where `isHoliday == "Y"`.
    async fn fetch_korea_holiday_set(
        &self,
        start_year: i32,
        end_year: i32,
    ) -> anyhow::Result<HashSet<NaiveDate>>;
}
