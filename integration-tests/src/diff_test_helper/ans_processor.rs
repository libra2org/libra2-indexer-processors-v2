use crate::models::ans_models::{CurrentAnsLookupV2, CurrentAnsPrimaryNameV2};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{
    current_ans_lookup_v2::dsl as cal_v2_dsl, current_ans_primary_name_v2::dsl as capn_v2_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let cal_v2_result = cal_v2_dsl::current_ans_lookup_v2
        .filter(cal_v2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(cal_v2_dsl::registered_address.asc())
        .then_order_by(cal_v2_dsl::token_standard.asc())
        .load::<CurrentAnsLookupV2>(conn)?;
    result_map.insert(
        "current_ans_lookup_v2".to_string(),
        serde_json::to_value(&cal_v2_result)?,
    );

    let capn_v2_result = capn_v2_dsl::current_ans_primary_name_v2
        .filter(capn_v2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(capn_v2_dsl::registered_address.asc())
        .then_order_by(capn_v2_dsl::token_standard.asc())
        .load::<CurrentAnsPrimaryNameV2>(conn)?;
    result_map.insert(
        "current_ans_primary_name_v2".to_string(),
        serde_json::to_value(&capn_v2_result)?,
    );

    Ok(result_map)
}
