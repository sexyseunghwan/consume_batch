use crate::common::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_keyword::*;

use crate::repository::mysql_repository::*;

use crate::schema::{CONSUME_PRODT_DETAIL, CONSUME_PRODT_KEYWORD, CONSUMUE_KEYWORD_TYPE};

use crate::schema::CONSUME_PRODT_DETAIL::dsl::*;

pub trait QueryService {
    // fn consume_keyword_type_join_consume_prodt_keyword(
    //     &self,
    // ) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
    fn get_top_consume_prodt_detail_order_by_timestamp(
        &self,
        top_n: i64,
        ascending: bool,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
    fn get_total_count_consume_prodt_detail(&self) -> Result<i64, anyhow::Error>;
    fn get_all_consume_prodt_detail(&self) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
    fn get_all_consume_prodt_type(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
}

#[derive(Debug, new)]
pub struct QueryServicePub;

impl QueryService for QueryServicePub {
    #[doc = "Functions that select all objects in the 'ConsumeProdtKeyword' table"]
    fn get_all_consume_prodt_type(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
        let mut conn = get_mysql_pool()?;

        let query = CONSUME_PRODT_KEYWORD::table.select((
            CONSUME_PRODT_KEYWORD::consume_keyword_type,
            CONSUME_PRODT_KEYWORD::consume_keyword,
        ));

        let result = query.load::<ConsumeProdtKeyword>(&mut conn)?;

        Ok(result)
    }

    #[doc = "Functions that select all objects in the 'ConsumeProdtDetail' table"]
    fn get_all_consume_prodt_detail(&self) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error> {
        let mut conn = get_mysql_pool()?;

        let query = CONSUME_PRODT_DETAIL::table.select((
            CONSUME_PRODT_DETAIL::timestamp,
            CONSUME_PRODT_DETAIL::cur_timestamp,
            CONSUME_PRODT_DETAIL::prodt_name,
            CONSUME_PRODT_DETAIL::prodt_money,
            CONSUME_PRODT_DETAIL::reg_dt.nullable(),
            CONSUME_PRODT_DETAIL::chg_dt.nullable(),
            CONSUME_PRODT_DETAIL::reg_id.nullable(),
            CONSUME_PRODT_DETAIL::chg_id.nullable(),
        ));

        let result = query.load::<ConsumeProdtDetail>(&mut conn)?;

        Ok(result)
    }

    // #[doc = ""]
    // fn consume_keyword_type_join_consume_prodt_keyword(
    //     &self,
    // ) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
    //     let mut conn = get_mysql_pool()?;

    //     let result = CONSUME_PRODT_KEYWORD::table
    //         .inner_join(
    //             CONSUMUE_KEYWORD_TYPE::table.on(CONSUME_PRODT_KEYWORD::consume_keyword_type
    //                 .eq(CONSUMUE_KEYWORD_TYPE::consume_keyword_type)),
    //         )
    //         .select((
    //             CONSUME_PRODT_KEYWORD::consume_keyword_type,
    //             CONSUME_PRODT_KEYWORD::consume_keyword,
    //         ))
    //         .load::<ConsumeProdtKeyword>(&mut conn)?;

    //     Ok(result)
    // }

    #[doc = "Function that returns the top N pieces of data after sorting the data in the 'ConsumeProdtDetail' table by the 'cur_timestamp' column"]
    /// # Arguments
    /// * `top_n` - Number to specify as top
    /// * `ascending` - Is it in ascending order
    ///
    /// # Returns
    /// * Result<Vec<ConsumeProdtDetail>, anyhow::Error>
    fn get_top_consume_prodt_detail_order_by_timestamp(
        &self,
        top_n: i64,
        ascending: bool,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error> {
        let mut conn = get_mysql_pool()?;

        let query = CONSUME_PRODT_DETAIL::table.select((
            CONSUME_PRODT_DETAIL::timestamp,
            CONSUME_PRODT_DETAIL::cur_timestamp,
            CONSUME_PRODT_DETAIL::prodt_name,
            CONSUME_PRODT_DETAIL::prodt_money,
            CONSUME_PRODT_DETAIL::reg_dt.nullable(),
            CONSUME_PRODT_DETAIL::chg_dt.nullable(),
            CONSUME_PRODT_DETAIL::reg_id.nullable(),
            CONSUME_PRODT_DETAIL::chg_id.nullable(),
        ));

        let result: Vec<ConsumeProdtDetail>;

        if ascending {
            result = QueryDsl::limit(
                QueryDsl::order(query, CONSUME_PRODT_DETAIL::cur_timestamp.asc()),
                top_n,
            )
            .load::<ConsumeProdtDetail>(&mut conn)?
        } else {
            result = QueryDsl::limit(
                QueryDsl::order(query, CONSUME_PRODT_DETAIL::cur_timestamp.desc()),
                top_n,
            )
            .load::<ConsumeProdtDetail>(&mut conn)?
        }

        Ok(result)
    }

    #[doc = "Functions that return the number of data present in the table 'CONSUME_PRODT_DETAIL'"]
    fn get_total_count_consume_prodt_detail(&self) -> Result<i64, anyhow::Error> {
        let mut conn = get_mysql_pool()?;

        let count = CONSUME_PRODT_DETAIL
            .select(count_star())
            .first::<i64>(&mut conn)?;

        Ok(count)
    }
}
