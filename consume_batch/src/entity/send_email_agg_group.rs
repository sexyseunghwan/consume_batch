//! `SeaORM` Entity for SEND_EMAIL_AGG_GROUP table

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "SEND_EMAIL_AGG_GROUP")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub agg_group_seq: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub email_id: String,
    pub is_active: bool,
    pub created_at: DateTime,
    pub updated_at: Option<DateTime>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::agg_group::Entity",
        from = "Column::AggGroupSeq",
        to = "super::agg_group::Column::AggGroupSeq"
    )]
    AggGroup,
    #[sea_orm(
        belongs_to = "super::users_email::Entity",
        from = "Column::EmailId",
        to = "super::users_email::Column::EmailId"
    )]
    UsersEmail,
}

impl Related<super::agg_group::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::AggGroup.def()
    }
}

impl Related<super::users_email::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UsersEmail.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
