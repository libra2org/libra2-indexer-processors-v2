use crate::{
    config::processor_config::DefaultProcessorConfig,
    db::{
        models::user_transaction_models::{
            signatures::Signature, user_transactions::PostgresUserTransaction,
        },
        queries::user_transaction_queries::{
            insert_signatures_query, insert_user_transactions_query,
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

pub struct UserTransactionStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl UserTransactionStorer {
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
impl Processable for UserTransactionStorer {
    type Input = (Vec<PostgresUserTransaction>, Vec<Signature>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<PostgresUserTransaction>, Vec<Signature>)>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (user_txns, signatures) = input.data;

        let user_txns = filter_data(
            &self.tables_to_write,
            TableFlags::USER_TRANSACTIONS,
            user_txns,
        );
        let signatures = filter_data(&self.tables_to_write, TableFlags::SIGNATURES, signatures);

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let ut_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_user_transactions_query,
            &user_txns,
            get_config_table_chunk_size::<PostgresUserTransaction>(
                "user_transactions",
                &per_table_chunk_sizes,
            ),
        );
        let s_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_signatures_query,
            &signatures,
            get_config_table_chunk_size::<Signature>("signatures", &per_table_chunk_sizes),
        );

        futures::try_join!(ut_res, s_res)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionStorer {}

impl NamedStep for UserTransactionStorer {
    fn name(&self) -> String {
        "UserTransactionStorer".to_string()
    }
}
