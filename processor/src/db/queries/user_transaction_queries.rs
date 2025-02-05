use crate::{
    db::models::user_transaction_models::{
        signatures::Signature, user_transactions::PostgresUserTransaction,
    },
    schema,
};
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_user_transactions_query(
    items_to_insert: Vec<PostgresUserTransaction>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::user_transactions::dsl::*;
    (
        diesel::insert_into(schema::user_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                entry_function_contract_address.eq(excluded(entry_function_contract_address)),
                entry_function_module_name.eq(excluded(entry_function_module_name)),
                entry_function_function_name.eq(excluded(entry_function_function_name)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

pub fn insert_signatures_query(
    items_to_insert: Vec<Signature>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::signatures::dsl::*;
    (
        diesel::insert_into(schema::signatures::table)
            .values(items_to_insert)
            .on_conflict((
                transaction_version,
                multi_agent_index,
                multi_sig_index,
                is_sender_primary,
            ))
            .do_nothing(),
        None,
    )
}
