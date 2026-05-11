use crate::common::*;
use crate::enums::RangeOperator;

/// Query options for group-sequence range searches with sum aggregation.
///
/// Bundles the Elasticsearch index, range filter, sort, aggregation, and group
/// filter values used by `find_info_filter_groupseq_orderby_aggs_range`.
pub struct GroupSeqAggsRangeQuery<'a> {
    /// Elasticsearch index or alias to query.
    pub index_name: &'a str,
    /// Date or numeric field used in the range filter.
    pub range_field: &'a str,
    /// Lower bound value for the range filter.
    pub start_date: DateTime<Utc>,
    /// Upper bound value for the range filter.
    pub end_date: DateTime<Utc>,
    /// Elasticsearch range operator for `start_date`.
    pub start_op: RangeOperator,
    /// Elasticsearch range operator for `end_date`.
    pub end_op: RangeOperator,
    /// Field used to sort matched documents.
    pub order_by_field: &'a str,
    /// Whether to sort ascending.
    pub asc_yn: bool,
    /// Numeric field to sum in the aggregation.
    pub aggs_field: &'a str,
    /// Aggregate group sequence used in the term filter.
    pub group_seq: i64,
    
    pub query_size: i64
}
