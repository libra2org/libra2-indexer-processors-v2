use crate::{
    db::{
        models::ans_models::{
            ans_lookup_v2::PostgresCurrentAnsLookupV2,
            ans_primary_name_v2::PostgresCurrentAnsPrimaryNameV2,
        },
        queries::ans_processor_queries::{
            insert_current_ans_lookups_v2_query, insert_current_ans_primary_names_v2_query,
        },
    },
    processors::ans_processor::AnsProcessorConfig,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct AnsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: AnsProcessorConfig,
}

impl AnsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: AnsProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for AnsStorer {
    type Input = (
        Vec<PostgresCurrentAnsLookupV2>,
        Vec<PostgresCurrentAnsPrimaryNameV2>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresCurrentAnsLookupV2>,
            Vec<PostgresCurrentAnsPrimaryNameV2>,
        )>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (current_ans_lookups_v2, current_ans_primary_names_v2) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.default.per_table_chunk_sizes.clone();

        let cal_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_lookups_v2_query,
            &current_ans_lookups_v2,
            get_config_table_chunk_size::<PostgresCurrentAnsLookupV2>(
                "current_ans_lookup_v2",
                &per_table_chunk_sizes,
            ),
        );
        let capn_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_primary_names_v2_query,
            &current_ans_primary_names_v2,
            get_config_table_chunk_size::<PostgresCurrentAnsPrimaryNameV2>(
                "current_ans_primary_name_v2",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(cal_v2, capn_v2)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsStorer {}

impl NamedStep for AnsStorer {
    fn name(&self) -> String {
        "AnsStorer".to_string()
    }
}
