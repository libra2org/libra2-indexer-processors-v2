use crate::{
    db::models::object_models::v2_objects::{PostgresCurrentObject, PostgresObject},
    schema,
};
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_objects_query(
    items_to_insert: Vec<PostgresObject>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::objects::dsl::*;
    (
        diesel::insert_into(schema::objects::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_update()
            .set((inserted_at.eq(excluded(inserted_at)),)),
        None,
    )
}

pub fn insert_current_objects_query(
    items_to_insert: Vec<PostgresCurrentObject>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_objects::dsl::*;
    (
        diesel::insert_into(schema::current_objects::table)
            .values(items_to_insert)
            .on_conflict(object_address)
            .do_update()
            .set((
                owner_address.eq(excluded(owner_address)),
                state_key_hash.eq(excluded(state_key_hash)),
                allow_ungated_transfer.eq(excluded(allow_ungated_transfer)),
                last_guid_creation_num.eq(excluded(last_guid_creation_num)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                is_deleted.eq(excluded(is_deleted)),
                inserted_at.eq(excluded(inserted_at)),
                untransferrable.eq(excluded(untransferrable)),
            )),
        Some(
            " WHERE current_objects.last_transaction_version <= excluded.last_transaction_version ",
        ),
    )
}
