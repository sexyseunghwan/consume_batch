#[derive(Debug, Clone, Copy)]
pub enum RangeOperator {
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl RangeOperator {
    pub fn to_str(self) -> &'static str {
        match self {
            RangeOperator::GreaterThanOrEqual => "gte",
            RangeOperator::LessThanOrEqual => "lte",
        }
    }
}
