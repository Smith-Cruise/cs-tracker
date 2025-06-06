use serde::Serialize;
use serde_json::{json, Value};
use std::cmp::min;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

const API_ENDPOINT: &str = "https://api.csqaq.com/api/v1/goods/getPriceByMarketHashName";

#[derive(Serialize)]
struct BatchPricesRequestData<'a> {
    marketHashNameList: &'a [String],
}


pub async fn get_good_details(secret_key: &String, hash_names: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut start_offset = 0;
    let total_num = hash_names.len();
    
    loop {
        sleep(Duration::from_millis(1500)).await;
        if start_offset == total_num { 
            break;
        }
        let end_index = min(start_offset + 50, hash_names.len());
        let slice = &hash_names[start_offset..end_index];
        let filename = format!("{}-{}.json", start_offset, end_index);
        match get_batch_prices(filename, secret_key, slice).await {
            Err(err) => {
                println!("fetch failed {}, retry {}~{}", err, start_offset, end_index);
                continue;
            },
            Ok(..) => {
                println!("fetch {}~{} succeed", start_offset, end_index);
                start_offset = end_index;
            }
        }
        
    }
    
    Ok(())
}


async fn get_batch_prices(
    filename: String,
    secret_key: &String,
    hash_name: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let request_data = BatchPricesRequestData {
        marketHashNameList: hash_name,
    };
    let body = json!(request_data).to_string();
    let content = client
        .post(API_ENDPOINT)
        .header("ApiToken", secret_key)
        .body(body)
        .send()
        .await?
        .text()
        .await?;
    let v: Value = serde_json::from_str(&content)?;
    fs::write(format!("prices/{}.json", filename), content)?;
    Ok(())
}

