#[derive(Debug, Clone, Copy)]
pub enum RangeOperator {
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl RangeOperator {
    pub fn to_str(self) -> &'static str {
        match self {
            RangeOperator::GreaterThanOrEqual => "gte",
            RangeOperator::LessThan => "lt",
            RangeOperator::LessThanOrEqual => "lte",
        }
    }
}
