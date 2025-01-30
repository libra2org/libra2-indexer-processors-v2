// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::{GetTimeStamp, HasVersion, NamedTable},
    db::models::new_default_models::{
        current_table_items::{CurrentTableItem, CurrentTableItemConvertible},
        table_items::{TableItem, TableItemConvertible},
    },
    utils::util::{hash_str, standardize_address},
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetTableItem {
    pub txn_version: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl NamedTable for ParquetTableItem {
    const TABLE_NAME: &'static str = "table_items";
}

impl HasVersion for ParquetTableItem {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for ParquetTableItem {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetCurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetCurrentTableItem {
    const TABLE_NAME: &'static str = "current_table_items";
}

impl HasVersion for ParquetCurrentTableItem {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for ParquetCurrentTableItem {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetTableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl NamedTable for ParquetTableMetadata {
    const TABLE_NAME: &'static str = "table_metadata";
}

impl HasVersion for ParquetTableMetadata {
    fn version(&self) -> i64 {
        0 // This is a placeholder value to avoid a compile error
    }
}

impl GetTimeStamp for ParquetTableMetadata {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

// TODO: Currently used by parquet write set change, we need to remove this, and make write set change to use the base model instead.
impl ParquetTableItem {
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, ParquetCurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: write_table_item.key.to_string(),
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                is_deleted: false,
                block_timestamp,
            },
            ParquetCurrentTableItem {
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                key_hash: hash_str(&write_table_item.key.to_string()),
                key: write_table_item.key.to_string(),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                last_transaction_version: txn_version,
                is_deleted: false,
                block_timestamp,
            },
        )
    }

    pub fn from_delete_table_item(
        delete_table_item: &DeleteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, ParquetCurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: delete_table_item.key.to_string(),
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                is_deleted: true,
                block_timestamp,
            },
            ParquetCurrentTableItem {
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                key_hash: hash_str(&delete_table_item.key.to_string()),
                key: delete_table_item.key.to_string(),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                last_transaction_version: txn_version,
                is_deleted: true,
                block_timestamp,
            },
        )
    }
}

impl ParquetTableMetadata {
    pub fn from_write_table_item(table_item: &WriteTableItem) -> Self {
        Self {
            handle: table_item.handle.to_string(),
            key_type: table_item.data.as_ref().unwrap().key_type.clone(),
            value_type: table_item.data.as_ref().unwrap().value_type.clone(),
        }
    }
}

impl TableItemConvertible for ParquetTableItem {
    fn from_base(base_item: &TableItem) -> Self {
        ParquetTableItem {
            txn_version: base_item.txn_version,
            write_set_change_index: base_item.write_set_change_index,
            transaction_block_height: base_item.transaction_block_height,
            table_key: base_item.table_key.clone(),
            table_handle: base_item.table_handle.clone(),
            decoded_key: base_item.decoded_key.clone(),
            decoded_value: base_item.decoded_value.clone(),
            is_deleted: base_item.is_deleted,
            block_timestamp: base_item.block_timestamp,
        }
    }
}

impl CurrentTableItemConvertible for ParquetCurrentTableItem {
    fn from_base(base_item: &CurrentTableItem) -> Self {
        ParquetCurrentTableItem {
            table_handle: base_item.table_handle.clone(),
            key_hash: base_item.key_hash.clone(),
            key: base_item.key.clone(),
            decoded_key: base_item.decoded_key.clone(),
            decoded_value: base_item.decoded_value.clone(),
            last_transaction_version: base_item.last_transaction_version,
            is_deleted: base_item.is_deleted,
            block_timestamp: base_item.block_timestamp,
        }
    }
}
