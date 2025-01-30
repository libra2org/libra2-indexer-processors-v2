use crate::{
    db::models::{
        new_default_models::current_table_items::CurrentTableItem,
        old_default_models::postgres_move_tables::PostgresTableItem,
    },
    utils::util::{hash_str, standardize_address},
};
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};

/// TableItem is a struct that will be used to converted into Postgres or Parquet TableItem
pub struct TableItem {
    pub txn_version: i64,
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl TableItem {
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentTableItem) {
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
            CurrentTableItem {
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
    ) -> (Self, CurrentTableItem) {
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
            CurrentTableItem {
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

    pub fn postgres_table_item_from_write_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> PostgresTableItem {
        let (base_table_item, _current_table_item) = TableItem::from_write_table_item(
            write_table_item,
            write_set_change_index,
            txn_version,
            transaction_block_height,
            block_timestamp,
        );
        PostgresTableItem::from_base(&base_table_item)
    }
}

pub trait TableItemConvertible {
    fn from_base(base_item: &TableItem) -> Self;
}
