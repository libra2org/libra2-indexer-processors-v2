// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::models::account_transaction_models::account_transactions::PostgresAccountTransaction,
    schema,
};
use diesel::{pg::Pg, query_builder::QueryFragment};

pub fn insert_account_transactions_query(
    item_to_insert: Vec<PostgresAccountTransaction>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::account_transactions::dsl::*;

    (
        diesel::insert_into(schema::account_transactions::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, account_address))
            .do_nothing(),
        None,
    )
}
