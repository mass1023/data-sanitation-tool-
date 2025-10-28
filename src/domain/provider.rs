#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Provider {
    pub p_name: String,
}
