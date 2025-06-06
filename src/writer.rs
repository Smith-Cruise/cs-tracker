use std::fs;
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringBuilder, TimestampSecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
struct Item {
    goodId: i64,
    name: String,
    marketHashName: String,
    buffSellPrice: f64,
    buffSellNum: i64,
    yyypSellPrice: f64,
    yyypSellNum: i64,
    steamSellPrice: f64,
    steamSellNum: i64,
}

pub async fn write_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now().timestamp();
    // 写入 Parquet 文件
    let file = std::fs::File::create(format!("parquet/{}.parquet", now))?;
    let props = WriterProperties::builder().build();

    let write_schema = Schema::new(vec![
        Field::new("goodId", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("marketHashName", DataType::Utf8, false),
        Field::new("buffSellPrice", DataType::Float64, false),
        Field::new("buffSellNum", DataType::Int64, false),
        Field::new("yyypSellPrice", DataType::Float64, false),
        Field::new("yyypSellNum", DataType::Int64, false),
        Field::new("steamSellPrice", DataType::Float64, false),
        Field::new("steamSellNum", DataType::Int64, false),
        Field::new(
            "updateTime",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
    ]);

    let schema_ref = Arc::new(write_schema);

    let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), Some(props))?;

    for file in fs::read_dir("prices")? {
        let path = file?.path();
        let content = fs::read_to_string(&path)?;
        let v: Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(err) => {
                println!("ignore file: {}", path.display());
                continue;
            }
        };

        let mut good_ids = Int64Array::builder(1024);
        let mut names = StringBuilder::new();
        let mut market_hash_names = StringBuilder::new();
        let mut buff_sell_prices = Float64Array::builder(1024);
        let mut buff_sell_nums = Int64Array::builder(1024);
        let mut yyyp_sell_prices = Float64Array::builder(1024);
        let mut yyyp_sell_nums = Int64Array::builder(1024);
        let mut steam_sell_prices = Float64Array::builder(1024);
        let mut steam_sell_nums = Int64Array::builder(1024);
        let mut timestamps = TimestampSecondBuilder::new();

        for pair in v["data"]["success"].as_object().iter() {
            for (key, value) in pair.iter() {
                let item: Item = serde_json::from_value(value.clone())?;
                good_ids.append_value(item.goodId);
                names.append_value(item.name);
                market_hash_names.append_value(item.marketHashName);
                buff_sell_prices.append_value(item.buffSellPrice);
                buff_sell_nums.append_value(item.buffSellNum);
                yyyp_sell_prices.append_value(item.yyypSellPrice);
                yyyp_sell_nums.append_value(item.yyypSellNum);
                steam_sell_prices.append_value(item.steamSellPrice);
                steam_sell_nums.append_value(item.steamSellNum);
                timestamps.append_value(now);
            }
        }

        let col1 = Arc::new(good_ids.finish()) as ArrayRef;
        let col2 = Arc::new(names.finish()) as ArrayRef;
        let col3 = Arc::new(market_hash_names.finish()) as ArrayRef;
        let col4 = Arc::new(buff_sell_prices.finish()) as ArrayRef;
        let col5 = Arc::new(buff_sell_nums.finish()) as ArrayRef;
        let col6 = Arc::new(yyyp_sell_prices.finish()) as ArrayRef;
        let col7 = Arc::new(yyyp_sell_nums.finish()) as ArrayRef;
        let col8 = Arc::new(steam_sell_prices.finish()) as ArrayRef;
        let col9 = Arc::new(steam_sell_nums.finish()) as ArrayRef;
        let col10 = Arc::new(timestamps.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema_ref.clone(),
            vec![col1, col2, col3, col4, col5, col6, col7, col8, col9, col10],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}