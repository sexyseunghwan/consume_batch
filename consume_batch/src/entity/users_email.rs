//! `SeaORM` Entity for USERS_EMAIL table

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "USERS_EMAIL")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub email_id: String,
    pub user_seq: i64,
    pub is_recv: bool,
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
    #[sea_orm(has_many = "super::send_email_agg_group::Entity")]
    SendEmailAggGroup,
}

impl Related<super::users::Entity> for Entity {
    /// Returns the relation definition to `USERS`.
    ///
    /// # Returns
    ///
    /// Returns the SeaORM relation definition for the user that owns this email
    /// address.
    fn to() -> RelationDef {
        Relation::Users.def()
    }
}

impl Related<super::send_email_agg_group::Entity> for Entity {
    /// Returns the relation definition to `SEND_EMAIL_AGG_GROUP`.
    ///
    /// # Returns
    ///
    /// Returns the SeaORM relation definition for monthly report recipient
    /// mappings associated with this email address.
    fn to() -> RelationDef {
        Relation::SendEmailAggGroup.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
