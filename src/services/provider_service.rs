use sqlx::{ MySqlPool };
use crate::domain::provider::Provider;

pub async fn get_provider(
    pool: &MySqlPool,
    p_id: u32
) -> Result<String, sqlx::Error> {
    let provider = sqlx
        ::query_as::<_, Provider>(
            r#"
        SELECT p_id, p_name
        FROM providers
        WHERE enabled = 1 
        AND p_id = ?
        "#
        )
        .bind(p_id)
        .fetch_one(pool).await?;
    
    Ok(provider.p_name)
}
