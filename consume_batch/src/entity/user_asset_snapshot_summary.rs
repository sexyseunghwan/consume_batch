//! `SeaORM` Entity for USER_ASSET_SNAPSHOT_SUMMARY table

use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "USER_ASSET_SNAPSHOT_SUMMARY")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub summary_seq: i64,
    pub user_seq: i64,
    pub aggregated_at: DateTime,
    pub total_asset_amount: Decimal,
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
}

impl Related<super::users::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Users.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
