//! `SeaORM` Entity for CRYPTO table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "CRYPTO")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub crypto_seq: i64,
    pub crypto_name: String,
    pub crypto_price: Decimal,
    pub api_symbol: String,
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
    #[sea_orm(has_many = "super::crypto_asset::Entity")]
    CryptoAsset,
}

impl Related<super::currency_code::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CurrencyCode.def()
    }
}

impl Related<super::crypto_asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CryptoAsset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
