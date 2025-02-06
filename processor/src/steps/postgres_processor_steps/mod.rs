use crate::utils::table_flags::TableFlags;
pub mod account_restoration_processor;
pub mod account_transactions_processor;
pub mod ans_processor;
pub mod default_processor;
pub mod events_processor;
pub mod objects_processor;
pub mod stake_processor;
pub mod token_v2_processor;
pub mod user_transaction_processor;

/**
 * This is a helper function to filter data based on the tables_to_write set.
 * If the tables_to_write set is empty or contains the flag, return the data so that they are written to the database.
 * Otherwise, return an empty vector so that they are not written to the database.
 */
pub fn filter_data<T>(tables_to_write: &TableFlags, flag: TableFlags, data: Vec<T>) -> Vec<T> {
    if tables_to_write.is_empty() || tables_to_write.contains(flag) {
        data
    } else {
        vec![]
    }
}
