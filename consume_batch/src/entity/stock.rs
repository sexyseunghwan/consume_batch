//! `SeaORM` Entity for STOCK table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "STOCK")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub stock_seq: i64,
    pub market_seq: i64,
    pub stock_name: String,
    pub api_symbol: String,
    pub stock_price: Option<Decimal>,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::stock_type::Entity",
        from = "Column::MarketSeq",
        to = "super::stock_type::Column::MarketSeq"
    )]
    StockType,
    #[sea_orm(has_many = "super::stock_asset::Entity")]
    StockAsset,
}

impl Related<super::stock_type::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::StockType.def()
    }
}

impl Related<super::stock_asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::StockAsset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
