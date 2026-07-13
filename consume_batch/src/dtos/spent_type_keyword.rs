//! Spent type keyword model for Elasticsearch indexing.
//!
//! This module provides the data structure for indexing keyword type information
//! into Elasticsearch, combining data from COMMON_CONSUME_KEYWORD_TYPE and
//! COMMON_CONSUME_PRODT_KEYWORD tables.

use crate::common::*;

/// Represents a keyword type document for Elasticsearch indexing.
///
/// This structure combines information from the keyword type table and
/// the product keyword table for full-text search capabilities.
///
/// # Fields
///
/// * `consume_keyword_type` - The type/category of the keyword
/// * `consume_keyword` - The actual keyword text
/// * `keyword_weight` - Weight/importance of the keyword (higher = more important)
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, new)]
pub struct SpentTypeKeyword {
    /// The keyword id type/category
    pub consume_keyword_type_id: i64,

    /// The keyword type/category
    pub consume_keyword_type: String,

    /// The actual keyword text
    pub consume_keyword: String,

    /// Weight/importance of the keyword
    pub keyword_weight: i32,
}

impl SpentTypeKeyword {
    // /// Creates a new `SpentTypeKeyword` instance.
    // ///
    // /// # Arguments
    // ///
    // /// * `consume_keyword_type` - The keyword type/category
    // /// * `consume_keyword` - The actual keyword text
    // /// * `keyword_weight` - Weight/importance of the keyword
    // ///
    // /// # Examples
    // ///
    // /// ```
    // /// let keyword = SpentTypeKeyword::new(
    // ///     "food".to_string(),
    // ///     "pizza".to_string(),
    // ///     10
    // /// );
    // /// ```
    // pub fn new(consume_keyword_type: String, consume_keyword: String, keyword_weight: i32) -> Self {
    //     Self {
    //         consume_keyword_type,
    //         consume_keyword,
    //         keyword_weight,
    //     }
    // }
}
