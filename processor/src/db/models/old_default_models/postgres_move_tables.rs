// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::models::new_default_models::{
        current_table_items::{CurrentTableItem, CurrentTableItemConvertible},
        table_items::{TableItem, TableItemConvertible},
        table_metadata::{TableMetadata, TableMetadataConvertible},
    },
    schema::{current_table_items, table_items, table_metadatas},
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// TODO: Move this model to the new default models when refactoring

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(table_handle, key_hash))]
#[diesel(table_name = current_table_items)]
pub struct PostgresCurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = table_items)]
pub struct PostgresTableItem {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub key: String,
    pub table_handle: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(handle))]
#[diesel(table_name = table_metadatas)]
pub struct PostgresTableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl TableItemConvertible for PostgresTableItem {
    fn from_base(base_item: &TableItem) -> Self {
        PostgresTableItem {
            transaction_version: base_item.txn_version,
            write_set_change_index: base_item.write_set_change_index,
            transaction_block_height: base_item.transaction_block_height,
            key: base_item.table_key.clone(),
            table_handle: base_item.table_handle.clone(),
            decoded_key: serde_json::from_str(base_item.decoded_key.as_str()).unwrap(),
            decoded_value: base_item
                .decoded_value
                .clone()
                .map(|v| serde_json::from_str(v.as_str()).unwrap()),
            is_deleted: base_item.is_deleted,
        }
    }
}

impl TableMetadataConvertible for PostgresTableMetadata {
    fn from_base(base_item: &TableMetadata) -> Self {
        PostgresTableMetadata {
            handle: base_item.handle.clone(),
            key_type: base_item.key_type.clone(),
            value_type: base_item.value_type.clone(),
        }
    }
}

impl CurrentTableItemConvertible for PostgresCurrentTableItem {
    fn from_base(base_item: &CurrentTableItem) -> Self {
        PostgresCurrentTableItem {
            table_handle: base_item.table_handle.clone(),
            key_hash: base_item.key_hash.clone(),
            key: base_item.key.clone(),
            decoded_key: serde_json::from_str(base_item.decoded_key.as_str()).unwrap(),
            decoded_value: base_item
                .decoded_value
                .clone()
                .map(|v| serde_json::from_str(v.as_str()).unwrap()),
            last_transaction_version: base_item.last_transaction_version,
            is_deleted: base_item.is_deleted,
        }
    }
}
