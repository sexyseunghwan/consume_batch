#[derive(Debug, Clone, Copy)]
pub enum RangeOperator {
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl RangeOperator {
    /// Converts the range operator into the Elasticsearch query keyword.
    ///
    /// Maps the enum variant to the string expected by Elasticsearch range
    /// queries.
    ///
    /// # Returns
    ///
    /// Returns `gte` or `lte` for the corresponding range operator.
    pub fn to_str(self) -> &'static str {
        match self {
            RangeOperator::GreaterThanOrEqual => "gte",
            RangeOperator::LessThanOrEqual => "lte",
        }
    }
}
