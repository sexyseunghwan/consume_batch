//! `SeaORM` Entity for CURRENCY_CODE table

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "CURRENCY_CODE")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub currency_code: String,
    pub currency_name: String,
    pub country_code: String,
    pub is_active: bool,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::stock_type::Entity")]
    StockType,
    #[sea_orm(has_many = "super::crypto::Entity")]
    Crypto,
}

impl Related<super::stock_type::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::StockType.def()
    }
}

impl Related<super::crypto::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Crypto.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
