use std::{io::Read, sync::Arc};

use datafusion::{assert_batches_eq, common::Result, execution::context::SessionContext};
use register::register_web_sources;

pub mod expr_to_sql;
pub mod register;
pub mod utils;
pub mod web_source;

/// Reads certificate and key pem files into a buffer which can be used to construct a rustls Identity
pub fn read_pem(file: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    std::fs::File::open(file)?.read_to_end(&mut buf)?;
    Ok(buf)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let access_level = "default_access";
    let client_cert = Arc::new(read_pem(&format!("./client_cert_{access_level}.pem")).unwrap());
    let client_key = Arc::new(read_pem(&format!("./client_key_{access_level}.pem")).unwrap());
    let ca_cert = Arc::new(read_pem("./cacert.pem").unwrap());
    let local_relay_endpoint = "https://localhost:50055";

    let ctx = SessionContext::new();
    let _providers = register_web_sources(
        &ctx,
        Arc::new(local_relay_endpoint.to_string()),
        client_cert.clone(),
        client_key.clone(),
        ca_cert.clone(),
    )
    .await?;

    let df = ctx
        .sql("select name from customer where name='hello from ballista!'")
        .await?;
    let expected = [
        "+----------------------+",
        "| name                 |",
        "+----------------------+",
        "| hello from ballista! |",
        "+----------------------+",
    ];

    assert_batches_eq!(expected, &df.clone().collect().await?);

    // Casting to BIGINT with a reasonable number of signficant digits to avoid errors due to
    // slight floating point rounding differences.
    let tpchq1 = "select
        returnflag,
        linestatus,
        CAST(sum(quantity) as BIGINT) as sum_qty,
        CAST(sum(extendedprice) AS BIGINT) as sum_base_price,
        CAST(sum(extendedprice * (1 - discount_percent/100)) AS BIGINT) as sum_disc_price,
        CAST(sum(extendedprice * (1 - discount_percent/100) * (1 + tax_percent/100)) AS BIGINT) as sum_charge,
        CAST(avg(quantity) AS BIGINT) as avg_qty,
        CAST(avg(extendedprice) AS BIGINT) as avg_price,
        CAST(avg(discount_percent*100) AS BIGINT) as avg_disc_pct,
        count(*) as count_order
    from
        lineitem
    where
            shipdate <= date '1998-09-02'
    group by
        returnflag,
        linestatus
    order by
        returnflag,
        linestatus";

    let df = ctx.sql(tpchq1).await?;

    let expected = [
        "+------------+------------+---------+----------------+----------------+------------+---------+-----------+--------------+-------------+",
        "| returnflag | linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge | avg_qty | avg_price | avg_disc_pct | count_order |",
        "+------------+------------+---------+----------------+----------------+------------+---------+-----------+--------------+-------------+",
        "| N          | F          | 44855   | 61924006       | 58991286       | 61412425   | 25      | 35588     | 477          | 1740        |",
        "| N          | O          | 3714106 | 5207661950     | 4948827476     | 5147237314 | 25      | 35691     | 499          | 145908      |",
        "+------------+------------+---------+----------------+----------------+------------+---------+-----------+--------------+-------------+"];

    assert_batches_eq!(expected, &df.clone().collect().await?);

    let tpchq3 = "select
    l.orderkey,
    CAST(sum(l.extendedprice * (1 - l.discount_percent/100)) as BIGINT) as revenue,
    o.orderdate,
    o.shippriority
    from
        customer c,
        orders o,
        lineitem l
    where
            c.mktsegment = 'BUILDING'
    and c.custkey = o.custkey
    and l.orderkey = o.orderkey
    and o.orderdate < date '1995-03-15'
    and l.shipdate > date '1995-03-15'
    group by
        l.orderkey,
        o.orderdate,
        o.shippriority
    order by
        revenue desc,
        o.orderdate
    limit 5";

    let df = ctx.sql(tpchq3).await?;

    let expected = [
        "+----------+----------+------------+--------------+",
        "| orderkey | revenue  | orderdate  | shippriority |",
        "+----------+----------+------------+--------------+",
        "| 27719    | 19269009 | 1995-02-14 | 0            |",
        "| 47714    | 18190751 | 1995-03-11 | 0            |",
        "| 10916    | 18105245 | 1995-03-11 | 0            |",
        "| 6022     | 16680923 | 1995-02-13 | 0            |",
        "| 32965    | 16246346 | 1995-02-25 | 0            |",
        "+----------+----------+------------+--------------+",
    ];

    assert_batches_eq!(expected, &df.clone().collect().await?);

    Ok(())
}
