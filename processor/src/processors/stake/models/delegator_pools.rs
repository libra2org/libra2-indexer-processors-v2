// Copyright © A-p-t-o-s Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::stake_utils::{StakeResource, StakeTableItem};
use crate::{
    schema::{
        current_delegated_staking_pool_balances, delegated_staking_pool_balances,
        delegated_staking_pools,
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use ahash::AHashMap;
use libra2_indexer_processor_sdk::{
    libra2_indexer_transaction_stream::utils::time::parse_timestamp,
   libra2_protos::transaction::v1::{
        transaction::TxnData, write_set_change::Change, Transaction, WriteResource, WriteTableItem,
    },
    utils::convert::standardize_address,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type StakingPoolAddress = String;
pub type DelegatorPoolMap = AHashMap<StakingPoolAddress, DelegatorPool>;
pub type DelegatorPoolBalanceMap = AHashMap<StakingPoolAddress, CurrentDelegatorPoolBalance>;

// All pools
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = delegated_staking_pools)]
pub struct DelegatorPool {
    pub staking_pool_address: String,
    pub first_transaction_version: i64,
}

// Metadata to fill pool balances and delegator balance
#[derive(Debug, Deserialize, Serialize)]
pub struct DelegatorPoolBalanceMetadata {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub active_share_table_handle: String,
    pub inactive_share_table_handle: String,
}

// Similar metadata but specifically for 0x1::pool_u64_unbound::Pool
#[derive(Debug, Deserialize, Serialize)]
pub struct PoolBalanceMetadata {
    pub transaction_version: i64,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub shares_table_handle: String,
    pub parent_table_handle: String,
}
pub trait PoolBalanceMetadataConvertible {
    fn from_base(base: PoolBalanceMetadata) -> Self;
}

// Pools balances
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, staking_pool_address))]
#[diesel(table_name = delegated_staking_pool_balances)]
pub struct DelegatorPoolBalance {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}
pub trait DelegatorPoolBalanceConvertible {
    fn from_base(base: DelegatorPoolBalance) -> Self;
}

// All pools w latest balances (really a more comprehensive version than DelegatorPool)
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_delegated_staking_pool_balances)]
pub struct CurrentDelegatorPoolBalance {
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub last_transaction_version: i64,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

impl DelegatorPool {
    pub fn from_transaction(
        transaction: &Transaction,
    ) -> anyhow::Result<(
        DelegatorPoolMap,
        Vec<DelegatorPoolBalance>,
        DelegatorPoolBalanceMap,
    )> {
        let mut delegator_pool_map = AHashMap::new();
        let mut delegator_pool_balances = vec![];
        let mut delegator_pool_balances_map = AHashMap::new();
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["DelegatorPool"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok((
                    delegator_pool_map,
                    delegator_pool_balances,
                    delegator_pool_balances_map,
                ));
            },
        };
        let txn_version = transaction.version as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");

        let block_timestamp = parse_timestamp(timestamp, txn_version).naive_utc();

        // Do a first pass to get the mapping of active_share table handles to staking pool addresses
        if let TxnData::User(_) = txn_data {
            let changes = &transaction
                .info
                .as_ref()
                .expect("Transaction info doesn't exist!")
                .changes;
            for wsc in changes {
                if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                    let maybe_write_resource =
                        Self::from_write_resource(write_resource, txn_version, block_timestamp)?;
                    if let Some((pool, pool_balances, current_pool_balances)) = maybe_write_resource
                    {
                        let staking_pool_address = pool.staking_pool_address.clone();
                        delegator_pool_map.insert(staking_pool_address.clone(), pool);
                        delegator_pool_balances.push(pool_balances);
                        delegator_pool_balances_map
                            .insert(staking_pool_address.clone(), current_pool_balances);
                    }
                }
            }
        }
        Ok((
            delegator_pool_map,
            delegator_pool_balances,
            delegator_pool_balances_map,
        ))
    }

    pub fn get_delegated_pool_metadata_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<DelegatorPoolBalanceMetadata>> {
        if let Some(StakeResource::DelegationPool(inner)) =
            StakeResource::from_write_resource(write_resource, txn_version, block_timestamp)?
        {
            let staking_pool_address = standardize_address(&write_resource.address.to_string());
            let total_coins = inner.active_shares.total_coins;
            let total_shares =
                &inner.active_shares.total_shares / &inner.active_shares.scaling_factor;
            Ok(Some(DelegatorPoolBalanceMetadata {
                transaction_version: txn_version,
                staking_pool_address,
                total_coins,
                total_shares,
                scaling_factor: inner.active_shares.scaling_factor,
                operator_commission_percentage: inner.operator_commission_percentage.clone(),
                active_share_table_handle: inner.active_shares.shares.inner.get_handle(),
                inactive_share_table_handle: inner.inactive_shares.get_handle(),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_inactive_pool_metadata_from_write_table_item(
        write_table_item: &WriteTableItem,
        txn_version: i64,
    ) -> anyhow::Result<Option<PoolBalanceMetadata>> {
        let table_item_data = write_table_item.data.as_ref().unwrap();

        if let Some(StakeTableItem::Pool(inner)) = &StakeTableItem::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            let total_coins = inner.total_coins.clone();
            let total_shares = &inner.total_shares / &inner.scaling_factor;
            Ok(Some(PoolBalanceMetadata {
                transaction_version: txn_version,
                total_coins,
                total_shares,
                scaling_factor: inner.scaling_factor.clone(),
                shares_table_handle: inner.shares.inner.get_handle(),
                parent_table_handle: standardize_address(&write_table_item.handle.to_string()),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, DelegatorPoolBalance, CurrentDelegatorPoolBalance)>> {
        if let Some(balance) = &Self::get_delegated_pool_metadata_from_write_resource(
            write_resource,
            txn_version,
            block_timestamp,
        )? {
            let staking_pool_address = balance.staking_pool_address.clone();
            let total_coins = balance.total_coins.clone();
            let total_shares = balance.total_shares.clone();
            let transaction_version = balance.transaction_version;
            Ok(Some((
                Self {
                    staking_pool_address: staking_pool_address.clone(),
                    first_transaction_version: transaction_version,
                },
                DelegatorPoolBalance {
                    transaction_version,
                    staking_pool_address: staking_pool_address.clone(),
                    total_coins: total_coins.clone(),
                    total_shares: total_shares.clone(),
                    operator_commission_percentage: balance.operator_commission_percentage.clone(),
                    inactive_table_handle: balance.inactive_share_table_handle.clone(),
                    active_table_handle: balance.active_share_table_handle.clone(),
                },
                CurrentDelegatorPoolBalance {
                    staking_pool_address,
                    total_coins,
                    total_shares,
                    last_transaction_version: transaction_version,
                    operator_commission_percentage: balance.operator_commission_percentage.clone(),
                    inactive_table_handle: balance.inactive_share_table_handle.clone(),
                    active_table_handle: balance.active_share_table_handle.clone(),
                },
            )))
        } else {
            Ok(None)
        }
    }
}

// Postgres models

// Metadata to fill pool balances and delegator balance
#[derive(Debug, Deserialize, Serialize)]
pub struct PostgresDelegatorPoolBalanceMetadata {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub active_share_table_handle: String,
    pub inactive_share_table_handle: String,
}

impl From<DelegatorPoolBalanceMetadata> for PostgresDelegatorPoolBalanceMetadata {
    fn from(base: DelegatorPoolBalanceMetadata) -> Self {
        Self {
            transaction_version: base.transaction_version,
            staking_pool_address: base.staking_pool_address,
            total_coins: base.total_coins,
            total_shares: base.total_shares,
            scaling_factor: base.scaling_factor,
            operator_commission_percentage: base.operator_commission_percentage,
            active_share_table_handle: base.active_share_table_handle,
            inactive_share_table_handle: base.inactive_share_table_handle,
        }
    }
}

// Similar metadata but specifically for 0x1::pool_u64_unbound::Pool
#[derive(Debug, Deserialize, Serialize)]
pub struct PostgresPoolBalanceMetadata {
    pub transaction_version: i64,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub shares_table_handle: String,
    pub parent_table_handle: String,
}

impl From<PoolBalanceMetadata> for PostgresPoolBalanceMetadata {
    fn from(base: PoolBalanceMetadata) -> Self {
        Self {
            transaction_version: base.transaction_version,
            total_coins: base.total_coins,
            total_shares: base.total_shares,
            scaling_factor: base.scaling_factor,
            shares_table_handle: base.shares_table_handle,
            parent_table_handle: base.parent_table_handle,
        }
    }
}

// Pools balances
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, staking_pool_address))]
#[diesel(table_name = delegated_staking_pool_balances)]
pub struct PostgresDelegatorPoolBalance {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

impl From<DelegatorPoolBalance> for PostgresDelegatorPoolBalance {
    fn from(base: DelegatorPoolBalance) -> Self {
        Self {
            transaction_version: base.transaction_version,
            staking_pool_address: base.staking_pool_address,
            total_coins: base.total_coins,
            total_shares: base.total_shares,
            operator_commission_percentage: base.operator_commission_percentage,
            inactive_table_handle: base.inactive_table_handle,
            active_table_handle: base.active_table_handle,
        }
    }
}

// All pools w latest balances (really a more comprehensive version than DelegatorPool)
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_delegated_staking_pool_balances)]
pub struct PostgresCurrentDelegatorPoolBalance {
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub last_transaction_version: i64,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

impl From<CurrentDelegatorPoolBalance> for PostgresCurrentDelegatorPoolBalance {
    fn from(base: CurrentDelegatorPoolBalance) -> Self {
        Self {
            staking_pool_address: base.staking_pool_address,
            total_coins: base.total_coins,
            total_shares: base.total_shares,
            last_transaction_version: base.last_transaction_version,
            operator_commission_percentage: base.operator_commission_percentage,
            inactive_table_handle: base.inactive_table_handle,
            active_table_handle: base.active_table_handle,
        }
    }
}
