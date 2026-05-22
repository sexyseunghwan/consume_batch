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
    #[sea_orm(has_many = "super::user_current_asset_snapshot::Entity")]
    UserCurrentAssetSnapshot,
    #[sea_orm(has_many = "super::cash_asset::Entity")]
    CashAsset,
    #[sea_orm(has_many = "super::deposit_asset::Entity")]
    DepositAsset,
    #[sea_orm(has_many = "super::saving_asset::Entity")]
    SavingAsset,
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

impl Related<super::user_current_asset_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UserCurrentAssetSnapshot.def()
    }
}

impl Related<super::cash_asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CashAsset.def()
    }
}

impl Related<super::deposit_asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DepositAsset.def()
    }
}

impl Related<super::saving_asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::SavingAsset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
