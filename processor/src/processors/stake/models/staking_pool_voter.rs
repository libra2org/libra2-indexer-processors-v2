// Copyright © A-p-t-o-s Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    processors::stake::models::stake_utils::StakeResource, schema::current_staking_pool_voter,
};
use ahash::AHashMap;
use libra2_indexer_processor_sdk::{
    libra2_indexer_transaction_stream::utils::time::parse_timestamp,
   libra2_protos::transaction::v1::{write_set_change::Change, Transaction},
    utils::convert::standardize_address,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type StakingPoolAddress = String;
pub type StakingPoolVoterMap = AHashMap<StakingPoolAddress, CurrentStakingPoolVoter>;

pub struct CurrentStakingPoolVoter {
    pub staking_pool_address: String,
    pub voter_address: String,
    pub last_transaction_version: i64,
    pub operator_address: String,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl CurrentStakingPoolVoter {
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<StakingPoolVoterMap> {
        let mut staking_pool_voters = AHashMap::new();

        let txn_version = transaction.version as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let block_timestamp = parse_timestamp(timestamp, txn_version).naive_utc();
        for wsc in &transaction.info.as_ref().unwrap().changes {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some(StakeResource::StakePool(inner)) = StakeResource::from_write_resource(
                    write_resource,
                    txn_version,
                    block_timestamp,
                )? {
                    let staking_pool_address =
                        standardize_address(&write_resource.address.to_string());
                    staking_pool_voters.insert(staking_pool_address.clone(), Self {
                        staking_pool_address,
                        voter_address: inner.get_delegated_voter(),
                        last_transaction_version: txn_version,
                        operator_address: inner.get_operator_address(),
                        block_timestamp,
                    });
                }
            }
        }

        Ok(staking_pool_voters)
    }
}

// Postgres models
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_staking_pool_voter)]
pub struct PostgresCurrentStakingPoolVoter {
    pub staking_pool_address: String,
    pub voter_address: String,
    pub last_transaction_version: i64,
    pub operator_address: String,
}

impl From<CurrentStakingPoolVoter> for PostgresCurrentStakingPoolVoter {
    fn from(base: CurrentStakingPoolVoter) -> Self {
        Self {
            staking_pool_address: base.staking_pool_address,
            voter_address: base.voter_address,
            last_transaction_version: base.last_transaction_version,
            operator_address: base.operator_address,
        }
    }
}
