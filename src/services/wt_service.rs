use sqlx::{ MySqlPool, Pool, MySql, Result };
use futures::{ Stream };
use crate::domain::wt::WT;

pub fn get_wt<'a>(
    pool: &'a MySqlPool,
    p_id: &'a u32,
    p_name: &'a String
) -> impl Stream<Item = Result<WT, sqlx::Error>> + 'a {
    sqlx::query_as::<_, WT>(
        r#"
        SELECT 
            wt_id, 
            wt_details, 
            p_id
        FROM wt
        WHERE 
        p_id = ?
        AND p_name != ?
        "#
    )
        .bind(p_id)
        .bind(p_name)
        .fetch(pool)
}

pub async fn update_wt_detail_batch_tx(
    pool: &Pool<MySql>,
    p_name: &str,
    wt_id_list: Vec<i64>
) -> Result<(), sqlx::Error> {
    if wt_id_list.is_empty() {
        return Ok(());
    }

    log::info!("ID list to update: {:#?}", wt_id_list);

    let mut query =
        format!(r#" Update wt 
            SET wt_details = JSON_REPLACE(wt_details, '$.PName', '{}') 
            WHERE wt_id in ("#, p_name);

    query.push_str(&"?,".repeat(wt_id_list.len()));
    query.pop();
    query.push(')');

    let mut sqlx_query = sqlx::query(&query);

    for wt_id in &wt_id_list {
        sqlx_query = sqlx_query.bind(wt_id);
    }

    match sqlx_query.execute(pool).await {
        Ok(rows_affected) => {
            log::info!("rows affected: {:?}", rows_affected.rows_affected());

            for wt_id in &wt_id_list {
                log::info!(target: "successful", "{}", wt_id);
            }

            Ok(())
        }
        Err(e) => {
            for w_id in &wt_id_list {
                log::error!(target: "failed", "{}", w_id);
            }

            Err(e)
        }
    }
}
