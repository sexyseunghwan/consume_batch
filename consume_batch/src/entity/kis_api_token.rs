//! `SeaORM` Entity for KIS_API_TOKEN table

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "KIS_API_TOKEN")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub access_token: String,
    pub token_expired_at: DateTime,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
