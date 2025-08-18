use chrono::{DateTime, Utc};
use sqlx::{
    Pool, Postgres,
    types::{Decimal, JsonValue},
};
use uuid::Uuid;

pub async fn insert(
    db: &Pool<Postgres>,
    id: Uuid,
    amount: Decimal,
    requested_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "insert into payments.log (id, amount, requested_at) values($1, $2, $3)",
        id,
        amount,
        requested_at
    )
    .execute(db)
    .await?;

    Ok(())
}

pub async fn set_processed_by(
    db: &Pool<Postgres>,
    id: Uuid,
    processed_by: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "update payments.log set processed_by = $2 where id = $1",
        id,
        processed_by
    )
    .execute(db)
    .await?;

    Ok(())
}

pub async fn summary(
    db: &Pool<Postgres>,
    processor_names: &Vec<String>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<JsonValue, sqlx::Error> {
    sqlx::query_scalar!(
        r#"with processors as (
	select
		unnest as name
	from
		unnest($1::text[])
),
summaries as (
	select
		processed_by as name,
		sum(amount) as total_amount,
		count(amount) as total_requests
	from
		payments.log
	where
		processed_by = any($1)
		and requested_at >= $2
		and requested_at < $3
	group by
		processed_by
)
select
	json_object_agg(
		p.name,
		json_build_object(
			'totalAmount', coalesce(s.total_amount, 0),
			'totalRequests', coalesce(s.total_requests, 0)
		)
	) as "summary!: JsonValue"
from
	processors p
	left join summaries s on p.name = s.name;
"#,
        processor_names,
        from,
        to
    )
    .fetch_one(db)
    .await
}
