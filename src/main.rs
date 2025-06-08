mod good_details;
mod good_list;
mod writer;

use std::{env, fs};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !fs::exists("items")? {
        fs::create_dir("items")?;
    }
    
    if !fs::exists("prices")? {
        fs::create_dir("prices")?;
    }
    
    if !fs::exists("parquet")? { 
        fs::create_dir("parquet")?;
    }

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: xx credential");
        std::process::exit(1);
    }
    let secret_key: String = args[1].clone();
    println!("credential is {}", secret_key);
    good_list::get_good_list(&secret_key).await?;
    let hash_names = good_list::extract_hash_names().await?;
    good_details::get_good_details(&secret_key, hash_names).await?;
    writer::write_parquet().await?;
    Ok(())
}
