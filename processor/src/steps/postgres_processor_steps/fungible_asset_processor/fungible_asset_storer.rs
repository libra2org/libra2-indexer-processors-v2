use crate::{
    config::processor_config::DefaultProcessorConfig,
    db::{
        models::{
            coin_models::coin_supply::CoinSupply,
            fungible_asset_models::{
                v2_fungible_asset_activities::PostgresFungibleAssetActivity,
                v2_fungible_asset_balances::{
                    PostgresCurrentUnifiedFungibleAssetBalance, PostgresFungibleAssetBalance,
                },
                v2_fungible_asset_to_coin_mappings::PostgresFungibleAssetToCoinMapping,
                v2_fungible_metadata::PostgresFungibleAssetMetadataModel,
            },
        },
        queries::fungible_asset_queries::{
            insert_coin_supply_query, insert_current_unified_fungible_asset_balances_v1_query,
            insert_current_unified_fungible_asset_balances_v2_query,
            insert_fungible_asset_activities_query, insert_fungible_asset_metadata_query,
            insert_fungible_asset_to_coin_mappings_query,
        },
    },
    steps::postgres_processor_steps::filter_data,
    utils::{
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        table_flags::TableFlags,
    },
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct FungibleAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl FungibleAssetStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: DefaultProcessorConfig,
        tables_to_write: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
            tables_to_write,
        }
    }
}

#[async_trait]
impl Processable for FungibleAssetStorer {
    type Input = (
        Vec<PostgresFungibleAssetActivity>,
        Vec<PostgresFungibleAssetMetadataModel>,
        Vec<PostgresFungibleAssetBalance>,
        (
            Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
            Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
        ),
        Vec<CoinSupply>,
        Vec<PostgresFungibleAssetToCoinMapping>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresFungibleAssetActivity>,
            Vec<PostgresFungibleAssetMetadataModel>,
            Vec<PostgresFungibleAssetBalance>,
            (
                Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
                Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
            ),
            Vec<CoinSupply>,
            Vec<PostgresFungibleAssetToCoinMapping>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (
            fungible_asset_activities,
            fungible_asset_metadata,
            _fungible_asset_balances,
            (current_unified_fab_v1, current_unified_fab_v2),
            coin_supply,
            fa_to_coin_mappings,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        // This is a filter to support writng to db for backfilling so that we only write to the tables that are specified in the processor config
        // Or by default we write to all tables if the tables_to_write in the config is empty.
        let current_unified_fab_v1: Vec<PostgresCurrentUnifiedFungibleAssetBalance> = filter_data(
            &self.tables_to_write,
            TableFlags::CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES,
            current_unified_fab_v1,
        );

        let current_unified_fab_v2 = filter_data(
            &self.tables_to_write,
            TableFlags::CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES,
            current_unified_fab_v2,
        );

        let coin_supply = filter_data(&self.tables_to_write, TableFlags::COIN_SUPPLY, coin_supply);

        let fungible_asset_activities = filter_data(
            &self.tables_to_write,
            TableFlags::FUNGIBLE_ASSET_ACTIVITIES,
            fungible_asset_activities,
        );

        let fungible_asset_metadata = filter_data(
            &self.tables_to_write,
            TableFlags::FUNGIBLE_ASSET_METADATA,
            fungible_asset_metadata,
        );

        let faa = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_activities_query,
            &fungible_asset_activities,
            get_config_table_chunk_size::<PostgresFungibleAssetActivity>(
                "fungible_asset_activities",
                &per_table_chunk_sizes,
            ),
        );
        let fam = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_metadata_query,
            &fungible_asset_metadata,
            get_config_table_chunk_size::<PostgresFungibleAssetMetadataModel>(
                "fungible_asset_metadata",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v1_query,
            &current_unified_fab_v1,
            get_config_table_chunk_size::<PostgresCurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v2_query,
            &current_unified_fab_v2,
            get_config_table_chunk_size::<PostgresCurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cs = execute_in_chunks(
            self.conn_pool.clone(),
            insert_coin_supply_query,
            &coin_supply,
            get_config_table_chunk_size::<CoinSupply>("coin_supply", &per_table_chunk_sizes),
        );
        let fatcm = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_to_coin_mappings_query,
            &fa_to_coin_mappings,
            get_config_table_chunk_size::<PostgresFungibleAssetToCoinMapping>(
                "fungible_asset_to_coin_mappings",
                &per_table_chunk_sizes,
            ),
        );
        let (faa_res, fam_res, cufab1_res, cufab2_res, cs_res, fatcm_res) =
            tokio::join!(faa, fam, cufab_v1, cufab_v2, cs, fatcm);
        for res in [faa_res, fam_res, cufab1_res, cufab2_res, cs_res, fatcm_res] {
            match res {
                Ok(_) => {},
                Err(e) => {
                    return Err(ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })
                },
            }
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}
