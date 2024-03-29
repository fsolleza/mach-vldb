#[tokio::main]
async fn main() {
    example_write().await;
    example_read().await;
}

async fn example_write() {
    let mut client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();
    let mut req = client.api_v3_write_lp("foobar");
    let string = concat!(
        "cpu,host=s1 usage=0.5\n",
        "cpu,host=s2 usage=0.2\n",
        "cpu,host=s1 usage=0.3\n",
        "cpu,host=s2 usage=0.4\n",
        "cpu,host=s1 usage=0.2\n",
        "cpu,host=s3 usage=0.4\n",
        "cpu,host=s2 usage=0.2\n",
        "cpu,host=s1 usage=0.5\n"
    );
    println!("{}", string);
    req.body(string).send().await.unwrap();
}

async fn example_read() {
    let mut client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();
    let query = "SELECT * FROM cpu WHERE usage > 0.2";
    let mut resp_bytes = client
        .api_v3_query_sql("foobar", query)
        .format(influxdb3_client::Format::Csv) // could be Json
        .send()
        .await
        .unwrap();
    let printable: &str = std::str::from_utf8(&resp_bytes).unwrap();
    println!("{}", printable);
}
