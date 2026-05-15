//! `SeaORM` Entity for CURRENCY_EXCHANGE_RATE_SNAPSHOT table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "CURRENCY_EXCHANGE_RATE_SNAPSHOT")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub exchange_rate_snapshot_seq: i64,
    pub base_currency_code: String,
    pub target_currency_code: String,
    pub base_amount: Decimal,
    pub exchange_rate: Decimal,
    pub is_active: bool,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
