//! `SeaORM` Entity for CRYPTO_ASSET table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "CRYPTO_ASSET")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub crypto_asset_seq: i64,
    pub crypto_cnt: Decimal,
    pub avg_purchase_price: Decimal,
    pub crypto_seq: i64,
    pub user_seq: i64,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::crypto::Entity",
        from = "Column::CryptoSeq",
        to = "super::crypto::Column::CryptoSeq"
    )]
    Crypto,
    #[sea_orm(
        belongs_to = "super::users::Entity",
        from = "Column::UserSeq",
        to = "super::users::Column::UserSeq"
    )]
    Users,
}

impl Related<super::crypto::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Crypto.def()
    }
}

impl Related<super::users::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Users.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
