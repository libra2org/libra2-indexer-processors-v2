use crate::processors::stake::{
    models::{
        current_delegated_voter::CurrentDelegatedVoter,
        delegator_activities::PostgresDelegatedStakingActivity,
        delegator_balances::{PostgresCurrentDelegatorBalance, PostgresDelegatorBalance},
        delegator_pools::{
            DelegatorPool, PostgresCurrentDelegatorPoolBalance, PostgresDelegatorPoolBalance,
        },
        proposal_votes::PostgresProposalVote,
        staking_pool_voter::PostgresCurrentStakingPoolVoter,
    },
    parse_stake_data,
};
use libra2_indexer_processor_sdk::{
   libra2_protos::transaction::v1::Transaction,
    postgres::utils::database::ArcDbPool,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use tracing::error;

pub struct StakeExtractor
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    query_retries: u32,
    query_retry_delay_ms: u64,
}

impl StakeExtractor {
    pub fn new(conn_pool: ArcDbPool, query_retries: u32, query_retry_delay_ms: u64) -> Self {
        Self {
            conn_pool,
            query_retries,
            query_retry_delay_ms,
        }
    }
}

#[async_trait]
impl Processable for StakeExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<PostgresCurrentStakingPoolVoter>,
        Vec<PostgresProposalVote>,
        Vec<PostgresDelegatedStakingActivity>,
        Vec<PostgresDelegatorBalance>,
        Vec<PostgresCurrentDelegatorBalance>,
        Vec<DelegatorPool>,
        Vec<PostgresDelegatorPoolBalance>,
        Vec<PostgresCurrentDelegatorPoolBalance>,
        Vec<CurrentDelegatedVoter>,
    );
    type RunType = AsyncRunType;

    /// Processes a batch of transactions and extracts relevant staking data.
    ///
    /// This function processes a batch of transactions, extracting various types of staking-related
    /// data such as current staking pool voters, proposal votes, delegated staking activities,
    /// delegator balances, and more. The extracted data is then returned in a `TransactionContext`
    /// for further processing or storage.
    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<PostgresCurrentStakingPoolVoter>,
                Vec<PostgresProposalVote>,
                Vec<PostgresDelegatedStakingActivity>,
                Vec<PostgresDelegatorBalance>,
                Vec<PostgresCurrentDelegatorBalance>,
                Vec<DelegatorPool>,
                Vec<PostgresDelegatorPoolBalance>,
                Vec<PostgresCurrentDelegatorPoolBalance>,
                Vec<CurrentDelegatedVoter>,
            )>,
        >,
        ProcessorError,
    > {
        let conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {e:?}"),
                query: None,
            })?;

        let (
            raw_all_current_stake_pool_voters,
            raw_all_proposal_votes,
            raw_all_delegator_activities,
            raw_all_delegator_balances,
            raw_all_current_delegator_balances,
            all_delegator_pools,
            raw_all_delegator_pool_balances,
            raw_all_current_delegator_pool_balances,
            all_current_delegated_voter,
        ) = match parse_stake_data(
            &transactions.data,
            Some(conn),
            self.query_retries,
            self.query_retry_delay_ms,
        )
        .await
        {
            Ok(data) => data,
            Err(e) => {
                error!(
                    start_version = transactions.metadata.start_version,
                    end_version = transactions.metadata.end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error parsing stake data",
                );
                return Err(ProcessorError::ProcessError {
                    message: format!("Error parsing stake data: {e:?}"),
                });
            },
        };

        let all_delegator_balances: Vec<PostgresDelegatorBalance> = raw_all_delegator_balances
            .into_iter()
            .map(PostgresDelegatorBalance::from)
            .collect::<Vec<_>>();
        let all_current_delegator_balances = raw_all_current_delegator_balances
            .into_iter()
            .map(PostgresCurrentDelegatorBalance::from)
            .collect::<Vec<_>>();
        let all_delegator_pool_balances = raw_all_delegator_pool_balances
            .into_iter()
            .map(PostgresDelegatorPoolBalance::from)
            .collect::<Vec<_>>();
        let all_current_delegator_pool_balances = raw_all_current_delegator_pool_balances
            .into_iter()
            .map(PostgresCurrentDelegatorPoolBalance::from)
            .collect::<Vec<_>>();
        let all_delegator_activities = raw_all_delegator_activities
            .into_iter()
            .map(PostgresDelegatedStakingActivity::from)
            .collect::<Vec<_>>();
        let all_proposal_votes = raw_all_proposal_votes
            .into_iter()
            .map(PostgresProposalVote::from)
            .collect::<Vec<_>>();
        let all_current_stake_pool_voters = raw_all_current_stake_pool_voters
            .into_iter()
            .map(PostgresCurrentStakingPoolVoter::from)
            .collect::<Vec<_>>();

        Ok(Some(TransactionContext {
            data: (
                all_current_stake_pool_voters,
                all_proposal_votes,
                all_delegator_activities,
                all_delegator_balances,
                all_current_delegator_balances,
                all_delegator_pools,
                all_delegator_pool_balances,
                all_current_delegator_pool_balances,
                all_current_delegated_voter,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for StakeExtractor {}

impl NamedStep for StakeExtractor {
    fn name(&self) -> String {
        "StakeExtractor".to_string()
    }
}
