use tokio::time;
use std::time::{SystemTime, Duration};
use chrono::{DateTime, Utc, NaiveDateTime, format::SecondsFormat};

fn micros_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[tokio::main]
async fn main() {
    let micros = micros_since_epoch() as i64;
    let dt = DateTime::from_timestamp_micros(micros).unwrap();
    println!("{}", dt.to_rfc3339());
    example_write().await;
    example_read1().await;
    example_read2().await;
}

async fn example_write() {
    let mut client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();
    let data: Vec<&'static str> = vec![
        "cpu,host=s1 usage=0.5,foo=1.3",
        "cpu,host=s2 usage=0.2,foo=2.3",
        "cpu,host=s1 usage=0.3,foo=5.1",
        "cpu,host=s2 usage=0.4,foo=2.2",
        "cpu,host=s1 usage=0.2,foo=3.1",
        "cpu,host=s3 usage=0.4,foo=4.1",
        "cpu,host=s2 usage=0.2,foo=1.3",
        "cpu,host=s1 usage=0.5,foo=2.5"
    ];
    for item in data {
        let line = format!("{} {}u", item, micros_since_epoch());
        println!("{}", line);
        let mut req = client.api_v3_write_lp("foobar");
        req.body(item).send().await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn example_read1() {
    let mut client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();
    let lower = micros_since_epoch() - 3 * 1_000_000;
    let dt = DateTime::from_timestamp_micros(lower as i64).unwrap();
    let formatted = dt.to_rfc3339();
    let reverted: DateTime<Utc> = formatted.parse().unwrap();
    let query = format!(
        "SELECT * FROM cpu WHERE time > '{}'",
        dt.to_rfc3339()
    );
    println!("query: {}", query);
    let mut resp_bytes = client
        .api_v3_query_influxql("foobar", query)
        .format(influxdb3_client::Format::Csv) // could be Json
        .send()
        .await
        .unwrap();
    let printable: &str = std::str::from_utf8(&resp_bytes).unwrap();
    println!("{}", printable);
}

async fn example_read2() {
    let lower = micros_since_epoch() - 3 * 1_000_000;
    let dt = DateTime::from_timestamp_micros(lower as i64).unwrap();
    let formatted = dt.to_rfc3339();
    let mut client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();

    let query = format!(
        "SELECT COUNT(foo) FROM cpu WHERE time > '{}' GROUP BY time(1s),host fill(none)",
        formatted
    );


    let mut resp_bytes = client
        .api_v3_query_influxql("foobar", query)
        .format(influxdb3_client::Format::Csv) // could be Json
        .send()
        .await
        .unwrap();
    let printable: &str = std::str::from_utf8(&resp_bytes).unwrap();
    println!("{}", printable);
}
