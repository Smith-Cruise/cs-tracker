use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

const API_ENDPOINT: &str = "https://api.csqaq.com/api/v1/info/get_good_id";

#[derive(Serialize)]
pub struct GoodListRequestData {
    page_index: i64,
    page_size: i64,
}

pub async fn get_good_list(secret_key: &String) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut total_numbers: Option<u64> = Option::None;
    let mut current_total_numbers: u64 = 0;
    let mut is_finished = false;
    let mut page_index = 1;
    while !is_finished {
        sleep(Duration::from_millis(1500)).await;
        let request_data = GoodListRequestData {
            page_index: page_index,
            page_size: 500,
        };

        let content = match fetch_data(&client, &secret_key, &request_data).await {
            Ok(res) => res,
            Err(e) => {
                println!("page index: {} failed: {}, retry", page_index, e);
                continue;
            }
        };

        // println!("{}", content);
        let v: Value = match serde_json::from_str(&content) {
            Ok(res) => res,
            Err(e) => {
                println!("page index: {} failed: {}, retry", page_index, e);
                continue;
            }
        };

        match total_numbers {
            Some(n) => {
                current_total_numbers += v["data"]["data"]
                    .as_object()
                    .expect("failed to get data")
                    .len() as u64;
                if current_total_numbers >= n {
                    is_finished = true;
                }
            }
            None => {
                total_numbers = Some(v["data"]["total"].as_u64().expect("failed to get total"));
                current_total_numbers += v["data"]["data"]
                    .as_object()
                    .expect("failed to get data")
                    .len() as u64;
            }
        }
        
        fs::write(format!("items/page-{}.json", page_index), content)?;
        page_index += 1;
        println!("total numbers: {:?}, current total number: {}", total_numbers, current_total_numbers);
    }
    Ok(())
}

pub async fn extract_hash_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut hash_names = Vec::new();
    for file in fs::read_dir("items")? {
        let path = file?.path();
        let content = fs::read_to_string(path)?;
        let v: Value = serde_json::from_str(&content)?;
        for each in v["data"]["data"].as_object().iter() {
            for (_, value) in each.iter() {
                hash_names.push(value["market_hash_name"].as_str().unwrap().to_string());
            }
        }
    }
    Ok(hash_names)
}

async fn fetch_data(
    client: &Client,
    secret_key: &String,
    request_data: &GoodListRequestData,
) -> Result<String, Box<dyn std::error::Error>> {
    let body = json!(request_data).to_string();
    println!("Request: {}", body);
    let content = client
        .post(API_ENDPOINT)
        .header("ApiToken", secret_key)
        .body(body)
        .send()
        .await?
        .text()
        .await?;
    Ok(content)
}
