// Copyright © A-p-t-o-s Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_fungible_asset_balances::{get_paired_metadata_address, get_primary_fungible_store_address},
    v2_fungible_asset_utils::FungibleAssetStoreDeletionEvent,
};
use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        fungible_asset::{
            coin_models::{
                coin_activities::CoinActivity,
                coin_utils::{CoinEvent, EventGuidResource},
            },
            fungible_asset_models::v2_fungible_asset_utils::{FeeStatement, FungibleAssetEvent},
        },
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_activities,
};
use ahash::AHashMap;
use allocative::Allocative;
use anyhow::Context;
use libra2_indexer_processor_sdk::{
   libra2_protos::transaction::v1::{Event, TransactionInfo, UserTransactionRequest},
    utils::convert::{bigdecimal_to_u64, standardize_address},
};
use bigdecimal::{BigDecimal, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

pub const GAS_FEE_EVENT: &str = "0x1::libra2_coin::GasFeeEvent";
// We will never have a negative number on chain so this will avoid collision in postgres
pub const BURN_GAS_EVENT_CREATION_NUM: i64 = -1;
pub const BURN_GAS_EVENT_INDEX: i64 = -1;

pub type OwnerAddress = String;
pub type CoinType = String;
// Primary key of the current_coin_balances table, i.e. (owner_address, coin_type)
pub type CurrentCoinBalancePK = (OwnerAddress, CoinType);
pub type EventToCoinType = AHashMap<EventGuidResource, CoinType>;
pub type OwnerAddressToCoinType = AHashMap<String, String>;
pub type StoreAddressToDeletedFungibleAssetStoreEvent =
    AHashMap<String, FungibleAssetStoreDeletionEvent>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FungibleAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<BigDecimal>,
    pub event_type: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
}

impl FungibleAssetActivity {
    pub fn get_v2_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        txn_timestamp: chrono::NaiveDateTime,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        object_aggregated_data_mapping: &ObjectAggregatedDataMapping,
        store_address_to_deleted_fa_store_events: &StoreAddressToDeletedFungibleAssetStoreEvent,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.clone();
        if let Some(fa_event) =
            &FungibleAssetEvent::from_event(event_type.as_str(), &event.data, txn_version)?
        {
            let (storage_id, is_frozen, amount) = match fa_event {
                FungibleAssetEvent::WithdrawEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    None,
                    Some(inner.amount.clone()),
                ),
                FungibleAssetEvent::DepositEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    None,
                    Some(inner.amount.clone()),
                ),
                FungibleAssetEvent::FrozenEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    Some(inner.frozen),
                    None,
                ),
                FungibleAssetEvent::WithdrawEventV2(inner) => (
                    standardize_address(&inner.store),
                    None,
                    Some(inner.amount.clone()),
                ),
                FungibleAssetEvent::DepositEventV2(inner) => (
                    standardize_address(&inner.store),
                    None,
                    Some(inner.amount.clone()),
                ),
                FungibleAssetEvent::FrozenEventV2(inner) => {
                    (standardize_address(&inner.store), Some(inner.frozen), None)
                },
            };

            // Lookup the event address in the object_aggregated_data_mapping to get additional metadata
            // The events are emitted on the address of the fungible store.
            let mut maybe_owner_address = None;
            let mut maybe_asset_type = None;
            let maybe_object_metadata = object_aggregated_data_mapping.get(&storage_id);
            match maybe_object_metadata {
                Some(metadata) => {
                    // Get the store's owner address from ObjectCore.
                    maybe_owner_address = Some(metadata.object.object_core.get_owner_address());
                    // Get the store's asset type
                    maybe_asset_type = metadata
                        .fungible_asset_store
                        .as_ref()
                        .map(|fa| fa.metadata.get_reference_address());
                },
                None => {
                    // If the object metadata does not exist, it means the fungible store got deleted in the same transaction
                    // We need to get the owner address from the store_address_to_deleted_fa_store_events
                    let deleted_fa_store_event =
                        store_address_to_deleted_fa_store_events.get(&storage_id);
                    match deleted_fa_store_event {
                        Some(deleted_fa_store_event) => {
                            maybe_owner_address =
                                Some(standardize_address(&deleted_fa_store_event.owner));
                            maybe_asset_type =
                                Some(standardize_address(&deleted_fa_store_event.metadata));
                        },
                        None => {
                            // There might not be a deletion event if the transaction was committed before FungibleStoreDeletion
                            // events existed. Fallback to None.
                            tracing::warn!(
                                "Could not find fungible store deletion event in version {} for storage id: {}",
                                txn_version,
                                storage_id
                            );
                        },
                    }
                },
            }

            return Ok(Some(Self {
                transaction_version: txn_version,
                event_index,
                owner_address: maybe_owner_address,
                storage_id: storage_id.clone(),
                asset_type: maybe_asset_type,
                is_frozen,
                amount,
                event_type: event_type.clone(),
                is_gas_fee: false,
                gas_fee_payer_address: None,
                is_transaction_success: true,
                entry_function_id_str: entry_function_id_str.clone(),
                block_height,
                token_standard: TokenStandard::V2.to_string(),
                transaction_timestamp: txn_timestamp,
                storage_refund_amount: BigDecimal::zero(),
            }));
        }
        Ok(None)
    }

    pub fn get_v1_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        entry_function_id_str: &Option<String>,
        event_to_coin_type: &EventToCoinType,
        event_index: i64,
        address_to_coin_type: &OwnerAddressToCoinType,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) =
            CoinEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
        {
            let (owner_address, amount, coin_type_option) = match inner {
                CoinEvent::WithdrawCoinEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    inner.amount.clone(),
                    None,
                ),
                CoinEvent::DepositCoinEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    inner.amount.clone(),
                    None,
                ),
                CoinEvent::WithdrawCoinEventV2(inner) => (
                    standardize_address(&inner.account),
                    inner.amount,
                    Some(inner.coin_type.clone()),
                ),
                CoinEvent::DepositCoinEventV2(inner) => (
                    standardize_address(&inner.account),
                    inner.amount,
                    Some(inner.coin_type.clone()),
                ),
            };
            let coin_type = if let Some(coin_type) = coin_type_option {
                coin_type
            } else {
                let event_key = event.key.as_ref().context("event must have a key")?;
                let event_move_guid = EventGuidResource {
                    addr: standardize_address(event_key.account_address.as_str()),
                    creation_num: event_key.creation_number as i64,
                };
                // Given this mapping only contains coin type < 1000 length, we should not assume that the mapping exists.
                // If it doesn't exist, skip.
                // First try to get from event_to_coin_type mapping
                match event_to_coin_type.get(&event_move_guid) {
                    Some(coin_type) => coin_type.clone(),
                    None => {
                        // If not found, try to get from address_to_coin_type mapping
                        // This is temporary until we have a way to get the coin type from a new event
                        match address_to_coin_type.get(&event_move_guid.addr) {
                            Some(coin_type) => coin_type.clone(),
                            None => {
                                tracing::warn!(
                                    "Could not find coin type from either event or address mapping, version: {}, event guid: {:?}",
                                    txn_version,
                                    event_move_guid
                                );
                                return Ok(None);
                            },
                        }
                    },
                }
            };

            // Storage id should be derived (for the FA migration)
            let metadata_addr = get_paired_metadata_address(&coin_type);
            let storage_id = get_primary_fungible_store_address(&owner_address, &metadata_addr)
                .expect("calculate primary fungible store failed");
            Ok(Some(Self {
                transaction_version: txn_version,
                event_index,
                owner_address: Some(owner_address),
                storage_id,
                asset_type: Some(coin_type),
                is_frozen: None,
                amount: Some(amount),
                event_type: event.type_str.clone(),
                is_gas_fee: false,
                gas_fee_payer_address: None,
                is_transaction_success: true,
                entry_function_id_str: entry_function_id_str.clone(),
                block_height,
                token_standard: TokenStandard::V1.to_string(),
                transaction_timestamp,
                storage_refund_amount: BigDecimal::zero(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Artificially creates a gas event. If it's a fee payer, still show gas event to the sender
    /// but with an extra field to indicate the fee payer.
    pub fn get_gas_event(
        txn_info: &TransactionInfo,
        user_transaction_request: &UserTransactionRequest,
        entry_function_id_str: &Option<String>,
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        block_height: i64,
        fee_statement: Option<FeeStatement>,
    ) -> Self {
        let v1_activity = CoinActivity::get_gas_event(
            txn_info,
            user_transaction_request,
            entry_function_id_str,
            transaction_version,
            transaction_timestamp,
            block_height,
            fee_statement,
        );
        // Storage id should be derived (for the FA migration)
        let metadata_addr = get_paired_metadata_address(&v1_activity.coin_type);
        let storage_id =
            get_primary_fungible_store_address(&v1_activity.owner_address, &metadata_addr)
                .expect("calculate primary fungible store failed");
        Self {
            transaction_version,
            event_index: v1_activity.event_index.unwrap(),
            owner_address: Some(v1_activity.owner_address),
            storage_id,
            asset_type: Some(v1_activity.coin_type),
            is_frozen: None,
            amount: Some(v1_activity.amount),
            event_type: v1_activity.activity_type,
            is_gas_fee: v1_activity.is_gas_fee,
            gas_fee_payer_address: v1_activity.gas_fee_payer_address,
            is_transaction_success: v1_activity.is_transaction_success,
            entry_function_id_str: v1_activity.entry_function_id_str,
            block_height,
            token_standard: TokenStandard::V1.to_string(),
            transaction_timestamp,
            storage_refund_amount: v1_activity.storage_refund_amount,
        }
    }
}

// Parquet Model
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetFungibleAssetActivity {
    pub txn_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<String>, // it is a string representation of the u128
    pub event_type: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub storage_refund_octa: u64,
}

impl NamedTable for ParquetFungibleAssetActivity {
    const TABLE_NAME: &'static str = "fungible_asset_activities";
}

impl HasVersion for ParquetFungibleAssetActivity {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<FungibleAssetActivity> for ParquetFungibleAssetActivity {
    fn from(raw: FungibleAssetActivity) -> Self {
        Self {
            txn_version: raw.transaction_version,
            event_index: raw.event_index,
            owner_address: raw.owner_address,
            storage_id: raw.storage_id,
            asset_type: raw.asset_type,
            is_frozen: raw.is_frozen,
            amount: raw.amount.map(|v| v.to_string()),
            event_type: raw.event_type,
            is_gas_fee: raw.is_gas_fee,
            gas_fee_payer_address: raw.gas_fee_payer_address,
            is_transaction_success: raw.is_transaction_success,
            entry_function_id_str: raw.entry_function_id_str,
            block_height: raw.block_height,
            token_standard: raw.token_standard,
            block_timestamp: raw.transaction_timestamp,
            storage_refund_octa: bigdecimal_to_u64(&raw.storage_refund_amount),
        }
    }
}

// Postgres Model
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = fungible_asset_activities)]
pub struct PostgresFungibleAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<BigDecimal>,
    pub type_: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
}

impl From<FungibleAssetActivity> for PostgresFungibleAssetActivity {
    fn from(raw: FungibleAssetActivity) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            event_index: raw.event_index,
            owner_address: raw.owner_address,
            storage_id: raw.storage_id,
            asset_type: raw.asset_type,
            is_frozen: raw.is_frozen,
            amount: raw.amount,
            type_: raw.event_type,
            is_gas_fee: raw.is_gas_fee,
            gas_fee_payer_address: raw.gas_fee_payer_address,
            is_transaction_success: raw.is_transaction_success,
            entry_function_id_str: raw.entry_function_id_str,
            block_height: raw.block_height,
            token_standard: raw.token_standard,
            transaction_timestamp: raw.transaction_timestamp,
            storage_refund_amount: raw.storage_refund_amount,
        }
    }
}
