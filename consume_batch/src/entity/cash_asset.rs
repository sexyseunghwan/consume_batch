//! `SeaORM` Entity for CASH_ASSET table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "CASH_ASSET")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub cash_seq: i64,
    pub cash_name: String,
    pub cash: Decimal,
    pub user_seq: i64,
    pub currency_code: String,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::users::Entity",
        from = "Column::UserSeq",
        to = "super::users::Column::UserSeq"
    )]
    Users,
    #[sea_orm(
        belongs_to = "super::currency_code::Entity",
        from = "Column::CurrencyCode",
        to = "super::currency_code::Column::CurrencyCode"
    )]
    CurrencyCode,
}

impl Related<super::users::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Users.def()
    }
}

impl Related<super::currency_code::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CurrencyCode.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
