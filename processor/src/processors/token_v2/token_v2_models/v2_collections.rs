// Copyright © A-p-t-o-s Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::resources::FromWriteResource,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::{
            token_models::{
                token_utils::{CollectionDataIdType, TokenWriteSet},
                tokens::TableHandleToOwner,
            },
            token_v2_models::v2_token_utils::{Collection, TokenStandard},
        },
    },
    schema::{collections_v2, current_collections_v2},
};
use allocative_derive::Allocative;
use anyhow::Context;
use libra2_indexer_processor_sdk::{
   libra2_protos::transaction::v1::{WriteResource, WriteTableItem},
    postgres::utils::database::{DbContext, DbPoolConnection},
    utils::convert::standardize_address,
};
use bigdecimal::{BigDecimal, Zero};
use diesel::{prelude::*, sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// PK of current_collections_v2, i.e. collection_id
pub type CurrentCollectionV2PK = String;

pub const DEFAULT_CREATOR_ADDRESS: &str = "unknown";

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = collections_v2)]
pub struct CollectionV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub collection_properties: Option<serde_json::Value>,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(collection_id))]
#[diesel(table_name = current_collections_v2)]
pub struct CurrentCollectionV2 {
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub token_standard: String,
    pub collection_properties: Option<serde_json::Value>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, QueryableByName)]
pub struct CreatorFromCollectionTableV1 {
    #[diesel(sql_type = Text)]
    pub creator_address: String,
}

impl CollectionV2 {
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        if let Some(inner) = Collection::from_write_resource(write_resource)? {
            let (mut current_supply, mut max_supply, mut total_minted_v2) =
                (BigDecimal::zero(), None, None);
            let (mut mutable_description, mut mutable_uri) = (None, None);
            let mut collection_properties = serde_json::Value::Null;
            let address = standardize_address(&write_resource.address);
            if let Some(object_data) = object_metadatas.get(&address) {
                // Getting supply data (prefer fixed supply over unlimited supply although they should never appear at the same time anyway)
                let fixed_supply = object_data.fixed_supply.as_ref();
                let unlimited_supply = object_data.unlimited_supply.as_ref();
                if let Some(supply) = unlimited_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.clone(),
                        None,
                        Some(supply.total_minted.clone()),
                    );
                }
                if let Some(supply) = fixed_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.clone(),
                        Some(supply.max_supply.clone()),
                        Some(supply.total_minted.clone()),
                    );
                }

                // Aggregator V2 enables a separate struct for supply
                let concurrent_supply = object_data.concurrent_supply.as_ref();
                if let Some(supply) = concurrent_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.value.clone(),
                        if supply.current_supply.max_value == u64::MAX.into() {
                            None
                        } else {
                            Some(supply.current_supply.max_value.clone())
                        },
                        Some(supply.total_minted.value.clone()),
                    );
                }

                // Getting collection mutability config from Libra2Collection
                let collection = object_data.aptos_collection.as_ref();
                if let Some(collection) = collection {
                    mutable_description = Some(collection.mutable_description);
                    mutable_uri = Some(collection.mutable_uri);
                }

                collection_properties = object_data
                    .property_map
                    .as_ref()
                    .map(|m| m.inner.clone())
                    .unwrap_or(collection_properties);
            } else {
                // ObjectCore should not be missing, returning from entire function early
                return Ok(None);
            }

            let collection_id = address;
            let creator_address = inner.get_creator_address();
            let collection_name = inner.get_name_trunc();
            let description = inner.description.clone();
            let uri = inner.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: creator_address.clone(),
                    collection_name: collection_name.clone(),
                    description: description.clone(),
                    uri: uri.clone(),
                    current_supply: current_supply.clone(),
                    max_supply: max_supply.clone(),
                    total_minted_v2: total_minted_v2.clone(),
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    collection_properties: Some(collection_properties.clone()),
                    token_standard: TokenStandard::V2.to_string(),
                    transaction_timestamp: txn_timestamp,
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address,
                    collection_name,
                    description,
                    uri,
                    current_supply,
                    max_supply,
                    total_minted_v2,
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                    collection_properties: Some(collection_properties),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        db_context: &mut Option<DbContext<'_>>,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_collection_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::CollectionData(inner)) => Some(inner),
            _ => None,
        };
        // Lookup the collection table item from the table handle. This is used to get creator address to construct the
        // primary key. If we don't know the table handle, we can't get the collection metadata, so we need to do a db lookup.
        // In the future, we should deprecate the creation of collection v1's.
        if let Some(collection_data) = maybe_collection_data {
            let table_handle = table_item.handle.to_string();
            let maybe_creator_address = table_handle_to_owner
                .get(&standardize_address(&table_handle))
                .map(|table_metadata| table_metadata.get_owner_address());
            let mut creator_address = match maybe_creator_address {
                Some(ca) => ca,
                None => match db_context {
                    None => {
                        tracing::debug!(
                            transaction_version = txn_version,
                            lookup_key = &table_handle,
                            "Avoiding db lookup for Parquet."
                        );
                        DEFAULT_CREATOR_ADDRESS.to_string()
                    },
                    Some(db_context) => {
                        match Self::get_collection_creator_for_v1(
                            &mut db_context.conn,
                            &table_handle,
                            db_context.query_retries,
                            db_context.query_retry_delay_ms,
                        )
                        .await
                        {
                            Ok(ca) => ca,
                            Err(_) => {
                                tracing::warn!(
                                        transaction_version = txn_version,
                                        lookup_key = &table_handle,
                                        "Failed to get collection creator for table handle {table_handle}, txn version {txn_version}. You probably should backfill db."
                                    );
                                return Ok(None);
                            },
                        }
                    },
                },
            };
            creator_address = standardize_address(&creator_address);
            let collection_id_struct =
                CollectionDataIdType::new(creator_address, collection_data.get_name().to_string());
            let collection_id = collection_id_struct.to_id();
            let collection_name = collection_data.get_name_trunc();
            let uri = collection_data.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: collection_id_struct.creator.clone(),
                    collection_name: collection_name.clone(),
                    description: collection_data.description.clone(),
                    uri: uri.clone(),
                    current_supply: collection_data.supply.clone(),
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle.clone()),
                    token_standard: TokenStandard::V1.to_string(),
                    transaction_timestamp: txn_timestamp,
                    collection_properties: Some(serde_json::Value::Null),
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address: collection_id_struct.creator,
                    collection_name,
                    description: collection_data.description,
                    uri,
                    current_supply: collection_data.supply,
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle),
                    token_standard: TokenStandard::V1.to_string(),
                    collection_properties: Some(serde_json::Value::Null),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// If collection data is not in resources of the same transaction, then try looking for it in the database. Since collection owner
    /// cannot change, we can just look in the current_collection_datas table.
    /// Retrying a few times since this collection could've been written in a separate thread.
    async fn get_collection_creator_for_v1(
        conn: &mut DbPoolConnection<'_>,
        table_handle: &str,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<String> {
        let mut tried = 0;
        while tried < query_retries {
            tried += 1;
            match Self::get_by_table_handle(conn, table_handle).await {
                Ok(creator) => return Ok(creator),
                Err(_) => {
                    if tried < query_retries {
                        tokio::time::sleep(std::time::Duration::from_millis(query_retry_delay_ms))
                            .await;
                    }
                },
            }
        }
        Err(anyhow::anyhow!("Failed to get collection creator"))
    }

    /// TODO: Change this to a KV store
    async fn get_by_table_handle(
        conn: &mut DbPoolConnection<'_>,
        table_handle: &str,
    ) -> anyhow::Result<String> {
        let mut res: Vec<Option<CreatorFromCollectionTableV1>> = sql_query(
            "SELECT creator_address FROM current_collections_v2 WHERE table_handle_v1 = $1",
        )
        .bind::<Text, _>(table_handle)
        .get_results(conn)
        .await?;
        Ok(res
            .pop()
            .context("collection result empty")?
            .context("collection result null")?
            .creator_address)
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCollectionV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: String,          // BigDecimal
    pub max_supply: Option<String>,      // BigDecimal
    pub total_minted_v2: Option<String>, // BigDecimal
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub collection_properties: Option<String>, // json
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetCollectionV2 {
    const TABLE_NAME: &'static str = "collections_v2";
}

impl HasVersion for ParquetCollectionV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}
impl From<CollectionV2> for ParquetCollectionV2 {
    fn from(collection: CollectionV2) -> Self {
        ParquetCollectionV2 {
            txn_version: collection.transaction_version,
            write_set_change_index: collection.write_set_change_index,
            collection_id: collection.collection_id,
            creator_address: collection.creator_address,
            collection_name: collection.collection_name,
            description: collection.description,
            uri: collection.uri,
            current_supply: collection.current_supply.to_string(),
            max_supply: collection.max_supply.map(|v| v.to_string()),
            total_minted_v2: collection.total_minted_v2.map(|v| v.to_string()),
            mutable_description: collection.mutable_description,
            mutable_uri: collection.mutable_uri,
            table_handle_v1: collection.table_handle_v1,
            collection_properties: collection
                .collection_properties
                .map(|v| serde_json::to_string(&v).unwrap()),
            token_standard: collection.token_standard,
            block_timestamp: collection.transaction_timestamp,
        }
    }
}
