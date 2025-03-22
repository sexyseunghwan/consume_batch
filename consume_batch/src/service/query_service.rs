use crate::common::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_keyword::*;

use crate::repository::mysql_repository::*;

use crate::entity::{
  consume_prodt_keyword  
};

pub trait QueryService {
    async fn get_all_consume_prodt_type(&self, batch_size: usize) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
    fn get_top_consume_prodt_detail_order_by_timestamp(
        &self,
        top_n: i64,
        ascending: bool,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
    fn get_total_count_consume_prodt_detail(&self) -> Result<i64, anyhow::Error>;
    fn get_all_consume_prodt_detail(&self) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;    
}

#[derive(Debug, new)]
pub struct QueryServicePub;

impl QueryService for QueryServicePub {
    #[doc = "Functions that select all objects in the 'ConsumeProdtKeyword' table"]
    async fn get_all_consume_prodt_type(&self, batch_size: usize) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let mut total_consume_prodt_keyword: Vec<ConsumeProdtKeyword> = Vec::new();
        let mut last_keyword_type: Option<String> = None;
        let mut last_keyword: Option<String> = None;

        loop {
            let mut query: Select<consume_prodt_keyword::Entity> = consume_prodt_keyword::Entity::find()
                .order_by_asc(consume_prodt_keyword::Column::ConsumeKeywordType)
                .order_by_asc(consume_prodt_keyword::Column::ConsumeKeyword)
                .limit(batch_size as u64)
                .select_only()
                .columns([consume_prodt_keyword::Column::ConsumeKeywordType, consume_prodt_keyword::Column::ConsumeKeyword]);

            if let (Some(last_type), Some(last_keyword_val)) = (&last_keyword_type, &last_keyword) {
                query = query.filter(
                    Condition::all()
                        .add(consume_prodt_keyword::Column::ConsumeKeywordType.eq(last_type.clone()))
                        .add(consume_prodt_keyword::Column::ConsumeKeyword.gt(last_keyword_val.clone())),
                );
            }

            let mut batch_data: Vec<ConsumeProdtKeyword> = query
                .into_model()
                .all(db)
                .await
                .map_err(|e| anyhow!("[Error][get_all_consume_prodt_type()] {:?}", e))?;

            if batch_data.is_empty() {
                break;
            }

            total_consume_prodt_keyword.append(&mut batch_data);

            if let Some(last) = batch_data.last() {
                last_keyword_type = Some(last.consume_keyword_type.clone());
                last_keyword = Some(last.consume_keyword.clone());
            }
        }
        
        Ok(total_consume_prodt_keyword)
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
