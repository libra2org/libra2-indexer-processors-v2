pub mod account_restoration_models;
pub mod account_transaction_models;
pub mod ans_models;
pub mod coin_models;
pub mod default_models;
pub mod event_models;
pub mod fungible_asset_models;
pub mod object_models;
pub mod stake_models;
pub mod token_models;
pub mod token_v2_models;
pub mod transaction_metadata_models;
pub mod user_transaction_models;

// Default None value for parquet fields that are not set
const DEFAULT_NONE: &str = "NULL";
