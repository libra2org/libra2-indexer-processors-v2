use crate::{
    db::models::stake_models::{
        current_delegated_voter::CurrentDelegatedVoter,
        delegator_activities::PostgresDelegatedStakingActivity,
        delegator_balances::{PostgresCurrentDelegatorBalance, PostgresDelegatorBalance},
        delegator_pools::{
            DelegatorPool, PostgresCurrentDelegatorPoolBalance, PostgresDelegatorPoolBalance,
        },
        proposal_votes::PostgresProposalVote,
        staking_pool_voter::PostgresCurrentStakingPoolVoter,
    },
    schema,
};
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_current_stake_pool_voter_query(
    items_to_insert: Vec<PostgresCurrentStakingPoolVoter>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_staking_pool_voter::dsl::*;

    (diesel::insert_into(schema::current_staking_pool_voter::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             staking_pool_address.eq(excluded(staking_pool_address)),
             voter_address.eq(excluded(voter_address)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             operator_address.eq(excluded(operator_address)),
         )),
     Some(
         " WHERE current_staking_pool_voter.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_proposal_votes_query(
    items_to_insert: Vec<PostgresProposalVote>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::proposal_votes::dsl::*;

    (
        diesel::insert_into(schema::proposal_votes::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, proposal_id, voter_address))
            .do_nothing(),
        None,
    )
}

pub fn insert_delegator_activities_query(
    items_to_insert: Vec<PostgresDelegatedStakingActivity>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_activities::dsl::*;

    (
        diesel::insert_into(schema::delegated_staking_activities::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_nothing(),
        None,
    )
}

pub fn insert_delegator_balances_query(
    items_to_insert: Vec<PostgresDelegatorBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegator_balances::dsl::*;

    (
        diesel::insert_into(schema::delegator_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

pub fn insert_current_delegator_balances_query(
    items_to_insert: Vec<PostgresCurrentDelegatorBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegator_balances::dsl::*;

    (diesel::insert_into(schema::current_delegator_balances::table)
         .values(items_to_insert)
         .on_conflict((delegator_address, pool_address, pool_type, table_handle))
         .do_update()
         .set((
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             shares.eq(excluded(shares)),
             parent_table_handle.eq(excluded(parent_table_handle)),
         )),
     Some(
         " WHERE current_delegator_balances.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_delegator_pools_query(
    items_to_insert: Vec<DelegatorPool>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_pools::dsl::*;

    (diesel::insert_into(schema::delegated_staking_pools::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             first_transaction_version.eq(excluded(first_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(
         " WHERE delegated_staking_pools.first_transaction_version >= EXCLUDED.first_transaction_version ",
     ),
    )
}

pub fn insert_delegator_pool_balances_query(
    items_to_insert: Vec<PostgresDelegatorPoolBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_pool_balances::dsl::*;

    (
        diesel::insert_into(schema::delegated_staking_pool_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, staking_pool_address))
            .do_nothing(),
        None,
    )
}

pub fn insert_current_delegator_pool_balances_query(
    items_to_insert: Vec<PostgresCurrentDelegatorPoolBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegated_staking_pool_balances::dsl::*;

    (diesel::insert_into(schema::current_delegated_staking_pool_balances::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             total_coins.eq(excluded(total_coins)),
             total_shares.eq(excluded(total_shares)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             operator_commission_percentage.eq(excluded(operator_commission_percentage)),
             inactive_table_handle.eq(excluded(inactive_table_handle)),
             active_table_handle.eq(excluded(active_table_handle)),
         )),
     Some(
         " WHERE current_delegated_staking_pool_balances.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_current_delegated_voter_query(
    item_to_insert: Vec<CurrentDelegatedVoter>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegated_voter::dsl::*;

    (diesel::insert_into(schema::current_delegated_voter::table)
         .values(item_to_insert)
         .on_conflict((delegation_pool_address, delegator_address))
         .do_update()
         .set((
             voter.eq(excluded(voter)),
             pending_voter.eq(excluded(pending_voter)),
             last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             table_handle.eq(excluded(table_handle)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(
         " WHERE current_delegated_voter.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}
