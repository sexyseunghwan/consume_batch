//! `SeaORM` Entity for STOCK_ASSET table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "STOCK_ASSET")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub stock_asset_seq: i64,
    pub stock_cnt: i64,
    pub avg_purchase_price: Decimal,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub user_seq: i64,
    pub stock_seq: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::stock::Entity",
        from = "Column::StockSeq",
        to = "super::stock::Column::StockSeq"
    )]
    Stock,
    #[sea_orm(
        belongs_to = "super::users::Entity",
        from = "Column::UserSeq",
        to = "super::users::Column::UserSeq"
    )]
    Users,
}

impl Related<super::stock::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Stock.def()
    }
}

impl Related<super::users::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Users.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
