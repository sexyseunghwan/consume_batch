// @generated automatically by Diesel CLI.

diesel::table! {
    CONSUME_PRODT_KEYWORD (consume_keyword_type, consume_keyword) {
        #[max_length = 100]
        consume_keyword_type -> Varchar,
        #[max_length = 200]
        consume_keyword -> Varchar,
    }
}

diesel::table! {
    CONSUMUE_KEYWORD_TYPE (consume_keyword_type) {
        #[max_length = 100]
        consume_keyword_type -> Varchar,
    }
}

diesel::table! {
    users (id) {
        id -> Integer,
        #[max_length = 255]
        username -> Varchar,
        #[max_length = 255]
        email -> Varchar,
        #[max_length = 255]
        password_hash -> Varchar,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    CONSUME_PRODT_KEYWORD,
    CONSUMUE_KEYWORD_TYPE,
    users,
);
