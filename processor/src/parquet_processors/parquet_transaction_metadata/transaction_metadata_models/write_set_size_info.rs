// Copyright © A-p-t-o-s Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::parquet_processors::parquet_utils::util::{HasVersion, NamedTable};
use allocative_derive::Allocative;
use libra2_indexer_processor_sdk::libra2_protos::transaction::v1::WriteOpSizeInfo;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetWriteSetSize {
    pub txn_version: i64,
    pub change_index: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub total_bytes: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetWriteSetSize {
    const TABLE_NAME: &'static str = "write_set_size";
}

impl HasVersion for ParquetWriteSetSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl ParquetWriteSetSize {
    pub fn from_transaction_info(
        info: &WriteOpSizeInfo,
        txn_version: i64,
        change_index: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        ParquetWriteSetSize {
            txn_version,
            change_index,
            key_bytes: info.key_bytes as i64,
            value_bytes: info.value_bytes as i64,
            total_bytes: info.key_bytes as i64 + info.value_bytes as i64,
            block_timestamp,
        }
    }
}
