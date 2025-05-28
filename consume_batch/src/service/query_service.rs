use crate::common::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_keyword::*;

use crate::repository::mysql_repository::*;

use crate::entity::{consume_prodt_detail, consume_prodt_keyword};

pub trait QueryService {
    async fn get_all_consume_prodt_type(
        &self,
        batch_size: usize,
    ) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
    async fn get_all_consume_prodt_detail(
        &self,
        batch_size: usize,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
    async fn get_top_consume_prodt_detail_order_by_timestamp(
        &self,
        top_n: usize,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
    async fn batch_insert<M>(
        &self,
        models: &Vec<M>,
        batch_size: usize,
    ) -> Result<usize, anyhow::Error>
    where
        M: ActiveModelTrait + Send + Sync + 'static;
}

#[derive(Debug, new)]
pub struct QueryServicePub;

impl QueryService for QueryServicePub {
    #[doc = "Functions that select all objects in the 'ConsumeProdtKeyword' table"]
    async fn get_all_consume_prodt_type(
        &self,
        batch_size: usize,
    ) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let mut total_consume_prodt_keyword: Vec<ConsumeProdtKeyword> = Vec::new();
        let mut last_keyword_type: Option<String> = None;
        let mut last_keyword: Option<String> = None;

        loop {
            let mut query: Select<consume_prodt_keyword::Entity> =
                consume_prodt_keyword::Entity::find()
                    .order_by_asc(consume_prodt_keyword::Column::ConsumeKeywordType)
                    .order_by_asc(consume_prodt_keyword::Column::ConsumeKeyword)
                    .limit(batch_size as u64)
                    .select_only()
                    .columns([
                        consume_prodt_keyword::Column::ConsumeKeywordType,
                        consume_prodt_keyword::Column::ConsumeKeyword,
                    ]);

            if let (Some(last_type), Some(last_keyword_val)) = (&last_keyword_type, &last_keyword) {
                query = query.filter(
                    Condition::any()
                        .add(
                            consume_prodt_keyword::Column::ConsumeKeywordType.gt(last_type.clone()),
                        )
                        .add(
                            Condition::all()
                                .add(
                                    consume_prodt_keyword::Column::ConsumeKeywordType
                                        .eq(last_type.clone()),
                                )
                                .add(
                                    consume_prodt_keyword::Column::ConsumeKeyword
                                        .gt(last_keyword_val.clone()),
                                ),
                        ),
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

            if let Some(last) = batch_data.last() {
                last_keyword_type = Some(last.consume_keyword_type.clone());
                last_keyword = Some(last.consume_keyword.clone());
            }

            total_consume_prodt_keyword.append(&mut batch_data);
        }

        Ok(total_consume_prodt_keyword)
    }

    #[doc = "Functions that select all objects in the 'ConsumeProdtDetail' table"]
    async fn get_all_consume_prodt_detail(
        &self,
        batch_size: usize,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let mut consume_prodt_details: Vec<ConsumeProdtDetail> = Vec::new();

        let mut last_prodt_name: Option<String> = None;
        let mut last_timestamp: Option<NaiveDateTime> = None;

        loop {
            let mut query: Select<consume_prodt_detail::Entity> =
                consume_prodt_detail::Entity::find()
                    .order_by_asc(consume_prodt_detail::Column::Timestamp)
                    .order_by_asc(consume_prodt_detail::Column::ProdtName)
                    .limit(batch_size as u64)
                    .select_only()
                    .columns([
                        consume_prodt_detail::Column::Timestamp,
                        consume_prodt_detail::Column::CurTimestamp,
                        consume_prodt_detail::Column::ProdtName,
                        consume_prodt_detail::Column::ProdtMoney,
                        consume_prodt_detail::Column::RegDt,
                        consume_prodt_detail::Column::ChgDt,
                        consume_prodt_detail::Column::RegDt,
                        consume_prodt_detail::Column::ChgDt,
                    ]);
            //test
            if let (Some(last_prodt_name), Some(last_timestamp)) =
                (&last_prodt_name, &last_timestamp)
            {
                query = query.filter(
                    // Condition::any()
                    //     .add(consume_prodt_detail::Column::Timestamp.gt(last_timestamp.clone()))
                    //     .add(
                    //         Condition::all()
                    //             .add(
                    //                 consume_prodt_detail::Column::Timestamp
                    //                     .eq(last_timestamp.clone()),
                    //             )
                    //             .add(
                    //                 consume_prodt_detail::Column::ProdtName
                    //                     .gt(last_prodt_name.clone()),
                    //             ),
                    //     ),
                    Condition::all()
                        .add( consume_prodt_detail::Column::Timestamp.eq(last_timestamp.clone()))
                        .add( consume_prodt_detail::Column::Timestamp.eq(last_prodt_name.clone()))
                );
            }

            let mut batch_data: Vec<ConsumeProdtDetail> = query
                .into_model()
                .all(db)
                .await
                .map_err(|e| anyhow!("[Error][get_all_consume_prodt_detail()] {:?}", e))?;

            if batch_data.is_empty() {
                break;
            }

            if let Some(last) = batch_data.last() {
                last_prodt_name = Some(last.prodt_name.clone());
                last_timestamp = Some(last.timestamp.clone());
            }

            consume_prodt_details.append(&mut batch_data);
        }

        Ok(consume_prodt_details)
    }

    // #[doc = ""]
    // async fn consume_keyword_type_join_consume_prodt_keyword(
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
    ///
    /// # Returns
    /// * Result<Vec<ConsumeProdtDetail>, anyhow::Error>
    async fn get_top_consume_prodt_detail_order_by_timestamp(
        &self,
        top_n: usize,
    ) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error> {
        if top_n > 10000 {
            return Err(anyhow!("[Error][get_top_consume_prodt_detail_order_by_timestamp()] Only up to the top 10000 data can be viewed."));
        }

        let db: &DatabaseConnection = establish_connection().await;

        let query: Select<consume_prodt_detail::Entity> = consume_prodt_detail::Entity::find()
            .select_only()
            .columns([
                consume_prodt_detail::Column::Timestamp,
                consume_prodt_detail::Column::CurTimestamp,
                consume_prodt_detail::Column::ProdtName,
                consume_prodt_detail::Column::ProdtMoney,
                consume_prodt_detail::Column::RegDt,
                consume_prodt_detail::Column::ChgDt,
                consume_prodt_detail::Column::RegDt,
                consume_prodt_detail::Column::ChgDt,
            ])
            .order_by_desc(consume_prodt_detail::Column::CurTimestamp)
            .limit(top_n as u64);

        let result: Vec<ConsumeProdtDetail> = query.into_model().all(db).await?;

        Ok(result)
    }

    #[doc = "Function that inserts data into the RDB using placement"]
    /// # Arguments
    /// * `models` - Data models targeted for insert
    ///
    /// # Returns
    /// * Result<InsertResult<Self::ActiveModel>, anyhow::Error>
    async fn batch_insert<M>(
        &self,
        models: &Vec<M>,
        batch_size: usize,
    ) -> Result<usize, anyhow::Error>
    where
        M: ActiveModelTrait + Send + Sync + 'static,
    {
        let db: &DatabaseConnection = establish_connection().await;

        let txn: DatabaseTransaction = db.begin().await?;

        let mut total_inserted: usize = 0;

        for chunk in models.chunks(batch_size) {
            let chunk_vec = chunk.to_vec();
            let result: std::result::Result<InsertResult<_>, sea_orm::DbErr> =
                M::Entity::insert_many(chunk_vec).exec(&txn).await;

            match result {
                Ok(_res) => {
                    total_inserted += chunk.len(); /* Accumulated number of inserts */
                }
                Err(e) => {
                    /* Rollback in case of error */
                    txn.rollback().await?;
                    return Err(anyhow!("[Error][QueryService -> batch_insert()] {:?}", e));
                }
            }
        }

        txn.commit().await?;

        Ok(total_inserted)
    }

    // #[doc = "Functions that return the number of data present in the table 'CONSUME_PRODT_DETAIL'"]
    // fn get_total_count_consume_prodt_detail(&self) -> Result<i64, anyhow::Error> {
    //     let mut conn = get_mysql_pool()?;

    //     let count = CONSUME_PRODT_DETAIL
    //         .select(count_star())
    //         .first::<i64>(&mut conn)?;

    //     Ok(count)
    // }
}
