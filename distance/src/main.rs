use aws_config::{meta::region::RegionProviderChain};
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use lambda_runtime::{service_fn, LambdaEvent, Error};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::query::QueryError;
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc, FixedOffset};

#[derive(Debug, Clone, Deserialize, Default)]
struct CustomEvent {
    imeis: String,
    input_ride_month: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CustomOutput {
    imei:String,
    ride_month: String,
    total_distance: f64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let func = service_fn(get_ride_data);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn get_ride_data(e: LambdaEvent<CustomEvent>) -> Result<Value, Error> {
    let payload = e.payload;

    if payload.imeis.is_empty() {
        println!("Imei cannot be empty");
        return Ok(json!({"error": "IMEI cannot be empty"}));
    }

    let region_provider = RegionProviderChain::first_try(Region::new("ap-south-1")).or_default_provider();
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let mut imei_month_distance: HashMap<(String, String), f64> = HashMap::new();
    let imeis: Vec<&str> = payload.imeis.split(',').collect(); 

    for imei in imeis{
        let items = match query_ride_new(&client, imei).await {
            Ok(items) => items.unwrap_or_default(),
            Err(err) => {
                eprintln!("Error querying consent config: {:?}", err);
                return Err(anyhow::anyhow!("Error querying consent config").into());
            }
        };

        for item in items.iter() {
            if item.get("ride_type").and_then(|v| v.as_s().ok()).unwrap_or(&"NA".to_string()) != "trip" {
                continue;
            }

            let ride_start = item.get("ride_start").and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok()).unwrap();

            let naive = NaiveDateTime::from_timestamp(ride_start as i64, 0);
            let offset = FixedOffset::east(5 * 3600 + 1800); 
            let datetime = Utc.from_utc_datetime(&naive).with_timezone(&offset);
            let ride_month = datetime.format("%Y-%m").to_string();

            let year_str = ride_month.split('-').next().unwrap();
            let year: u32 = year_str.parse().unwrap();
            if year==2024 || year == 2023{
                if let Some(input_month) = &payload.input_ride_month {
                    if &ride_month != input_month {
                        continue;
                    }
                }
                let ride_stats_map = item.get("ride_stats").and_then(|v| v.as_m().ok()).unwrap();

                let total_distance_str = ride_stats_map.get("ride_distance").and_then(|v| v.as_s().ok()).unwrap();
                let distance: f64 = total_distance_str.parse().unwrap_or(0.0);
                let key = (imei.to_string(), ride_month.clone());
                let value = imei_month_distance.entry(key).or_insert(0.0);
                *value += distance;
                
            }
        }
    }

    // Put data to new table
     for ((imei, ride_month), total_distance) in imei_month_distance.iter() {
        client.put_item()
            .table_name("ride_data_monthly_distance")
            .item("imei", AttributeValue::S(imei.clone()))
            .item("date", AttributeValue::S(ride_month.clone()))
            .item("total_distance", AttributeValue::N(total_distance.to_string()))
            .send()
            .await?;
    } 

    for ((imei, ride_month), total_distance) in imei_month_distance.iter(){
        println!("imei: {}", imei);
        println!("ride_month: {}", ride_month);
        println!("total_distance: {}", total_distance);
    }

    let output: Vec<CustomOutput> = imei_month_distance.into_iter().map(|((imei, ride_month), total_distance)| {
        CustomOutput {
            imei,
            ride_month,
            total_distance,
        }
    }).collect();

    Ok(json!(output))
}

async fn query_ride_new(
    client: &Client,
    imei: &str,
) -> Result<Option<Vec<HashMap<String, AttributeValue>>>, SdkError<QueryError>> {
    let imei_av = AttributeValue::S(imei.to_string());
   
    let resp = client
        .query()
        .table_name("ride_data")
        .key_condition_expression("#imei = :imei")
        .expression_attribute_names("#imei", "imei")
        .expression_attribute_values(":imei", imei_av)
        .projection_expression("ride_start, ride_stats, ride_type")
        .send()
        .await?;

    Ok(resp.items)
}