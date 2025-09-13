// Copyright © A-p-t-o-s Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    signature_utils::parent_signature_utils::{get_fee_payer_address, get_parent_signature_type},
    signatures::Signature,
};
use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::fungible_asset::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
    schema::user_transactions,
};
use allocative::Allocative;
use anyhow::Result;
use libra2_indexer_processor_sdk::{
    libra2_indexer_transaction_stream::utils::time::parse_timestamp,
   libra2_protos::{
        transaction::v1::{
            TransactionInfo, UserTransaction as UserTransactionPB, UserTransactionRequest,
        },
        util::timestamp::Timestamp,
    },
    utils::{
        convert::{bigdecimal_to_u64, standardize_address, u64_to_bigdecimal},
        extract::{
            get_entry_function_contract_address_from_user_request,
            get_entry_function_from_user_request,
            get_entry_function_function_name_from_user_request,
            get_entry_function_module_name_from_user_request,
            get_replay_protection_nonce_from_user_request,
        },
    },
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

/**
 * This is a base UserTransaction model struct which should be used to build all other extended models such as
 * ParquetUserTransaction, PostgresUserTransaction, etc.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UserTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    pub parent_signature_type: String,
    pub sender: String,
    pub sequence_number: Option<i64>,
    pub replay_protection_nonce: Option<BigDecimal>,
    pub max_gas_amount: BigDecimal,
    pub expiration_timestamp_secs: Timestamp,
    pub gas_unit_price: BigDecimal,
    pub gas_used_unit: u64,
    pub block_timestamp: chrono::NaiveDateTime,
    pub entry_function_id_str: String,
    pub epoch: i64,
    pub entry_function_contract_address: Option<String>,
    pub entry_function_module_name: Option<String>,
    pub entry_function_function_name: Option<String>,
    pub num_signatures: i64,
    pub is_transaction_success: bool,
    pub storage_refund_octa: u64,
    pub gas_fee_payer_address: Option<String>,
}

impl UserTransaction {
    pub fn from_transaction(
        txn: &UserTransactionPB,
        txn_info: &TransactionInfo,
        fee_statement: Option<FeeStatement>,
        timestamp: &Timestamp,
        block_height: i64,
        epoch: i64,
        version: i64,
    ) -> (Self, Vec<Signature>) {
        let user_request = txn
            .request
            .as_ref()
            .expect("Sends is not present in user txn");
        let gas_fee_payer_address = match user_request.signature.as_ref() {
            Some(signature) => get_fee_payer_address(signature, version),
            None => None,
        };
        let block_timestamp: chrono::NaiveDateTime =
            parse_timestamp(timestamp, version).naive_utc();
        let num_signatures =
            UserTransaction::get_signatures(user_request, version, block_height, block_timestamp)
                .len() as i64;

        // Extract nonce from payload's extra_config if available for orderless transactions
        let replay_protection_nonce = get_replay_protection_nonce_from_user_request(user_request);

        (
            Self {
                txn_version: version,
                block_height,
                parent_signature_type: txn
                    .request
                    .as_ref()
                    .unwrap()
                    .signature
                    .as_ref()
                    .map(get_parent_signature_type)
                    .unwrap_or_default(),
                sender: standardize_address(&user_request.sender),
                // For orderless transactions, we set sequence_number to None instead of u64::MAX
                sequence_number: replay_protection_nonce
                    .is_none()
                    .then_some(user_request.sequence_number as i64),
                replay_protection_nonce: replay_protection_nonce.map(u64_to_bigdecimal),
                max_gas_amount: u64_to_bigdecimal(user_request.max_gas_amount),
                expiration_timestamp_secs: user_request.expiration_timestamp_secs.unwrap(),
                gas_unit_price: u64_to_bigdecimal(user_request.gas_unit_price),
                block_timestamp,
                entry_function_id_str: get_entry_function_from_user_request(user_request)
                    .unwrap_or_default(),
                epoch,
                entry_function_contract_address: Some(
                    get_entry_function_contract_address_from_user_request(user_request)
                        .unwrap_or_default(),
                ),
                entry_function_module_name: Some(
                    get_entry_function_module_name_from_user_request(user_request)
                        .unwrap_or_default(),
                ),
                entry_function_function_name: Some(
                    get_entry_function_function_name_from_user_request(user_request)
                        .unwrap_or_default(),
                ),
                gas_used_unit: txn_info.gas_used,
                is_transaction_success: txn_info.success,
                storage_refund_octa: fee_statement
                    .map(|fs| fs.storage_fee_refund_octas)
                    .unwrap_or(0),
                gas_fee_payer_address,
                num_signatures, // Corrected to use the calculated number of signatures
            },
            Self::get_signatures(user_request, version, block_height, block_timestamp),
        )
    }

    /// Empty vec if signature is None
    pub fn get_signatures(
        user_request: &UserTransactionRequest,
        version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Vec<Signature> {
        user_request
            .signature
            .as_ref()
            .map(|s| {
                Signature::from_user_transaction(
                    s,
                    &user_request.sender,
                    version,
                    block_height,
                    block_timestamp,
                )
            })
            .unwrap_or_default()
    }
}

// Prevent conflicts with other things named `Transaction`
pub type UserTransactionModel = UserTransaction;

// Parquet version of UserTransaction
#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetUserTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub epoch: i64,
    pub sender: String,
    pub sequence_number: Option<i64>,
    pub replay_protection_nonce: Option<String>, // String format of BigDecimal
    pub entry_function_id_str: String,
    pub expiration_timestamp_secs: u64,
    pub parent_signature_type: String,
    pub gas_fee_payer_address: Option<String>,
    pub gas_used_unit: u64,
    pub gas_unit_price: u64,
    pub max_gas_octa: u64,
    pub storage_refund_octa: u64,
    pub is_transaction_success: bool,
    pub num_signatures: i64,
}

impl NamedTable for ParquetUserTransaction {
    const TABLE_NAME: &'static str = "user_transactions";
}

impl HasVersion for ParquetUserTransaction {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<UserTransaction> for ParquetUserTransaction {
    fn from(user_transaction: UserTransaction) -> Self {
        Self {
            txn_version: user_transaction.txn_version,
            block_height: user_transaction.block_height,
            block_timestamp: user_transaction.block_timestamp,
            epoch: user_transaction.epoch,
            sender: user_transaction.sender,
            sequence_number: user_transaction.sequence_number,
            replay_protection_nonce: user_transaction
                .replay_protection_nonce
                .map(|n| n.to_string()),
            entry_function_id_str: user_transaction.entry_function_id_str,
            expiration_timestamp_secs: user_transaction.expiration_timestamp_secs.seconds as u64,
            parent_signature_type: user_transaction.parent_signature_type,
            gas_fee_payer_address: user_transaction.gas_fee_payer_address,
            gas_used_unit: user_transaction.gas_used_unit,
            gas_unit_price: bigdecimal_to_u64(&user_transaction.gas_unit_price),
            max_gas_octa: bigdecimal_to_u64(&user_transaction.max_gas_amount),
            storage_refund_octa: user_transaction.storage_refund_octa,
            is_transaction_success: user_transaction.is_transaction_success,
            num_signatures: user_transaction.num_signatures,
        }
    }
}

// Postgres version of UserTransaction
#[derive(Clone, Deserialize, Debug, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(version))]
#[diesel(table_name = user_transactions)]
pub struct PostgresUserTransaction {
    pub version: i64,
    pub block_height: i64,
    pub parent_signature_type: String,
    pub sender: String,
    pub sequence_number: Option<i64>,
    pub replay_protection_nonce: Option<BigDecimal>,
    pub max_gas_amount: BigDecimal,
    pub expiration_timestamp_secs: chrono::NaiveDateTime,
    pub gas_unit_price: BigDecimal,
    pub timestamp: chrono::NaiveDateTime,
    pub entry_function_id_str: String,
    pub epoch: i64,
    pub entry_function_contract_address: Option<String>,
    pub entry_function_module_name: Option<String>,
    pub entry_function_function_name: Option<String>,
}

impl From<UserTransaction> for PostgresUserTransaction {
    fn from(user_transaction: UserTransaction) -> Self {
        Self {
            version: user_transaction.txn_version,
            block_height: user_transaction.block_height,
            parent_signature_type: user_transaction.parent_signature_type,
            sender: user_transaction.sender,
            sequence_number: user_transaction.sequence_number,
            replay_protection_nonce: user_transaction.replay_protection_nonce,
            max_gas_amount: user_transaction.max_gas_amount,
            expiration_timestamp_secs: parse_timestamp(
                &user_transaction.expiration_timestamp_secs,
                user_transaction.txn_version,
            )
            .naive_utc(),
            gas_unit_price: user_transaction.gas_unit_price,
            timestamp: user_transaction.block_timestamp,
            entry_function_id_str: user_transaction.entry_function_id_str,
            epoch: user_transaction.epoch,
            entry_function_contract_address: user_transaction.entry_function_contract_address,
            entry_function_module_name: user_transaction.entry_function_module_name,
            entry_function_function_name: user_transaction.entry_function_function_name,
        }
    }
}
