mod domain;
mod services;

use dotenv::dotenv;
use sqlx::mysql::MySqlPoolOptions;
use futures::TryStreamExt;
use services::{
    provider_service::get_provider,
    wt_service::{ get_wt, update_wt_detail_batch_tx },
};
use log::info;
use log4rs::init_file;
use std::env;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // load .env file
    dotenv().ok();
    // init logging with config file
    init_file("log4rs.yml", Default::default()).unwrap();

    info!("Starting the process");
    let p_id: u32 = env::var("p_id").ok().and_then(|s| s.parse().ok()).unwrap_or(9);
    let max_concurrent_updates = env::var("max_concurrent_updates").ok().and_then(|s| s.parse().ok()).unwrap_or(10);
    let batch_size = env::var("batch_size").ok().and_then(|s| s.parse().ok()).unwrap_or(100);
    let mysql_max_connections = env::var("mysql_max_connections").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let mysql_min_connections = env::var("mysql_min_connections").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let connection_string = env::var("connection_string")
        .unwrap_or_else(|_| "connectin_string".to_string());

    info!("Initializing database connection pool");
    let pool = MySqlPoolOptions::new()
        .max_connections(mysql_max_connections)
        .min_connections(mysql_min_connections)
        .connect(&connection_string).await?;

    let p_name = get_provider(&pool, p_id.clone()).await?;

    get_wt(&pool, &p_id, &p_name)
        .try_chunks(batch_size)
        .map_err(|e| e.1)
        .try_for_each_concurrent(max_concurrent_updates, |batch| {
            let pool = pool.clone();
            let name = p_name.clone();
            async move {
                let id_list: Vec<i64> = batch
                    .into_iter()
                    .map(|row| row.wt_id)
                    .collect();

                if !id_list.is_empty() {
                    update_wt_detail_batch_tx(
                        &pool,
                        &name,
                        id_list
                    ).await?;
                }
                Ok(())
            }
        }).await?;

    Ok(())
}
