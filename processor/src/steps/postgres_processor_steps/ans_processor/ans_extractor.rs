use crate::{
    config::processor_config::ProcessorConfig,
    db::models::ans_models::{
        ans_lookup_v2::PostgresCurrentAnsLookupV2,
        ans_primary_name_v2::PostgresCurrentAnsPrimaryNameV2,
    },
    parsing::ans_processor_helpers::parse_ans,
    processors::ans_processor::AnsProcessorConfig,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct AnsExtractor
where
    Self: Sized + Send + 'static,
{
    config: AnsProcessorConfig,
}

impl AnsExtractor {
    pub fn new(config: ProcessorConfig) -> Result<Self, anyhow::Error> {
        let processor_config = match config {
            ProcessorConfig::AnsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for ANS Processor: {:?}",
                    config
                ))
            },
        };

        Ok(Self {
            config: processor_config,
        })
    }
}

#[async_trait]
impl Processable for AnsExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<PostgresCurrentAnsLookupV2>,
        Vec<PostgresCurrentAnsPrimaryNameV2>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<PostgresCurrentAnsLookupV2>,
                Vec<PostgresCurrentAnsPrimaryNameV2>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            raw_current_ans_lookups_v2,
            _,
            raw_current_ans_primary_names_v2,
            _, // AnsPrimaryNameV2 is deprecated.
        ) = parse_ans(
            &input.data,
            self.config.ans_v1_primary_names_table_handle.clone(),
            self.config.ans_v1_name_records_table_handle.clone(),
            self.config.ans_v2_contract_address.clone(),
        );

        let postgres_current_ans_lookups_v2: Vec<PostgresCurrentAnsLookupV2> =
            raw_current_ans_lookups_v2
                .into_iter()
                .map(PostgresCurrentAnsLookupV2::from)
                .collect();

        let postgres_current_ans_primary_names_v2: Vec<PostgresCurrentAnsPrimaryNameV2> =
            raw_current_ans_primary_names_v2
                .into_iter()
                .map(PostgresCurrentAnsPrimaryNameV2::from)
                .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_current_ans_lookups_v2,
                postgres_current_ans_primary_names_v2,
            ),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsExtractor {}

impl NamedStep for AnsExtractor {
    fn name(&self) -> String {
        "AnsExtractor".to_string()
    }
}
