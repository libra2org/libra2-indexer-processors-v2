use crate::bq_analytics::{GetTimeStamp, HasVersion, NamedTable};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::WriteTableItem;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl TableMetadata {
    pub fn from_write_table_item(table_item: &WriteTableItem) -> Self {
        Self {
            handle: table_item.handle.to_string(),
            key_type: table_item.data.as_ref().unwrap().key_type.clone(),
            value_type: table_item.data.as_ref().unwrap().value_type.clone(),
        }
    }
}

pub trait TableMetadataConvertible {
    fn from_base(base_item: &TableMetadata) -> Self;
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
        -1
    }
}

impl GetTimeStamp for ParquetTableMetadata {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[allow(deprecated)]
        chrono::NaiveDateTime::from_timestamp(0, 0)
    }
}

impl TableMetadataConvertible for ParquetTableMetadata {
    fn from_base(base_item: &TableMetadata) -> Self {
        Self {
            handle: base_item.handle.clone(),
            key_type: base_item.key_type.clone(),
            value_type: base_item.value_type.clone(),
        }
    }
}
