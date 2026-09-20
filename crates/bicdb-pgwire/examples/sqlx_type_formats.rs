use sqlx::{Connection as _, Row as _};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args.next().ok_or("DATABASE_URL argument is required")?;
    let target = args.next().ok_or("TARGET argument is required")?;
    if args.next().is_some() {
        return Err("unexpected extra arguments".into());
    }

    let mut connection = sqlx::postgres::PgConnection::connect(&url).await?;
    let text = sqlx::query(
        "SELECT TRUE::text, (-12345)::int2::text, 123456789::int4::text, \
         9007199254740993::int8::text, 12345678901234567890.125::numeric::text, \
         '\\x00ff10'::bytea::text",
    )
    .fetch_one(&mut connection)
    .await?;
    assert_eq!(text.try_get::<String, _>(0)?, "true");
    assert_eq!(text.try_get::<String, _>(1)?, "-12345");
    assert_eq!(text.try_get::<String, _>(2)?, "123456789");
    assert_eq!(text.try_get::<String, _>(3)?, "9007199254740993");
    assert_eq!(text.try_get::<String, _>(4)?, "12345678901234567890.125");
    assert_eq!(text.try_get::<String, _>(5)?, "\\x00ff10");

    let binary = sqlx::query(
        "SELECT TRUE::bool, (-12345)::int2, 123456789::int4, \
         9007199254740993::int8, 1.5::float4, (-2.25)::float8, \
         'client-text'::text, '\\x00ff10'::bytea",
    )
    .fetch_one(&mut connection)
    .await?;
    assert!(binary.try_get::<bool, _>(0)?);
    assert_eq!(binary.try_get::<i16, _>(1)?, -12345);
    assert_eq!(binary.try_get::<i32, _>(2)?, 123456789);
    assert_eq!(binary.try_get::<i64, _>(3)?, 9007199254740993);
    assert_eq!(binary.try_get::<f32, _>(4)?, 1.5);
    assert_eq!(binary.try_get::<f64, _>(5)?, -2.25);
    assert_eq!(binary.try_get::<String, _>(6)?, "client-text");
    assert_eq!(binary.try_get::<Vec<u8>, _>(7)?, vec![0, 255, 16]);

    connection.close().await?;
    println!("SQLx text/binary gauntlet passed for {target}");
    Ok(())
}
