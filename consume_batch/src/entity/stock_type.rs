//! `SeaORM` Entity for STOCK_TYPE table

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "STOCK_TYPE")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub market_seq: i64,
    pub market: String,
    pub market_alias: String,
    pub currency_code: String,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::currency_code::Entity",
        from = "Column::CurrencyCode",
        to = "super::currency_code::Column::CurrencyCode"
    )]
    CurrencyCode,
    #[sea_orm(has_many = "super::stock::Entity")]
    Stock,
}

impl Related<super::currency_code::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CurrencyCode.def()
    }
}

impl Related<super::stock::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Stock.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
