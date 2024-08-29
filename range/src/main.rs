use aws_config::meta::region::RegionProviderChain;
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
use chrono::{TimeZone, Utc, FixedOffset};

#[derive(Debug, Clone, Deserialize, Default)]
struct CustomEvent {
    input_ride_month: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CustomOutput {
    imei:String,
    ride_month: String,
    total_range: f64,
}

#[tokio::main]
async fn main() -> Result<(),Error>{
    let func = service_fn(get_ride_data);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn get_ride_data(e: LambdaEvent<CustomEvent>) -> Result<Value, Error> {
    let payload = e.payload;

    let imeis_env = std::env::var("IMEIS").unwrap_or_else(|_| String::new());
    let imeis: Vec<&str> = imeis_env.split(',').collect();
    if imeis.is_empty() {
        println!("Imei cannot be empty");
        return Ok(json!({"error": "IMEI cannot be empty"}));
    }

    let region_provider = RegionProviderChain::first_try(Region::new("ap-south-1")).or_default_provider();
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let mut imei_month_range: HashMap<(String, String), f64> = HashMap::new();
    let mut imei_yearly_range: HashMap<String, f64> = HashMap::new();
    
    for imei in imeis {
        let items = match query_for_range(&client, imei).await {
            Ok(items) => items.unwrap_or_default(),
            Err(err) => {
                eprintln!("Error querying consent config: {:?}", err);
                return Err(anyhow::anyhow!("Error querying consent config").into());
            }
        };

        let mut curr_range: f64 = 0.0;
        let mut max_range: f64 = 0.0;
        let mut curr_range_yearly: f64 = 0.0;
        let mut max_range_yearly: f64 = 0.0;
        let mut in_trip: bool = false;
        let mut ride_month= String::new();
        let mut current_month = String::new();

        for item in items.iter() {
            let ride_type = item.get("ride_type").and_then(|v| v.as_s().ok()).unwrap();
            
            let ride_start = item.get("ride_start").and_then(|v| v.as_n().ok()).and_then(|s| s.parse::<u64>().ok()).unwrap();
            let utc_datetime = Utc.timestamp_opt(ride_start as i64, 0)
            .single().expect("Invalid timestamp");
            let offset = FixedOffset::east_opt(5 * 3600 + 1800).unwrap_or_else(|| {FixedOffset::east_opt(0).unwrap()});
            let datetime = utc_datetime.with_timezone(&offset);
            ride_month = datetime.format("%Y-%m").to_string();

            let year_str = ride_month.split('-').next().unwrap();
            let year: u32 = year_str.parse().unwrap();
            if year==2024 || year == 2023{
                if let Some(input_month) = &payload.input_ride_month {
                    if &ride_month != input_month {
                        continue;
                    }
                }
                if current_month.is_empty() {
                    current_month = ride_month.clone();
                }
                if ride_month != current_month {
                    if in_trip && curr_range > max_range {
                        max_range = curr_range;
                    }
                    let key = (imei.to_string(), current_month.clone());
                    let value: &mut f64 = imei_month_range.entry(key).or_insert(0.0);
                    if max_range > *value {
                        *value = max_range;
                    }
                    curr_range = 0.0;
                    max_range = 0.0;
                    current_month = ride_month.clone();
                }

                if ride_type == "charging" {
                    if in_trip {
                        if curr_range > max_range {
                            max_range = curr_range;
                        }
                        if curr_range_yearly > max_range_yearly {
                            max_range_yearly = curr_range_yearly;
                        }
                        curr_range = 0.0;
                        curr_range_yearly = 0.0;
                    }
                    in_trip = false; 
                } 
                else if ride_type == "trip" {
                    if !in_trip {
                        in_trip = true; 
                    }
                    let ride_stats_map = item.get("ride_stats").and_then(|v| v.as_m().ok()).unwrap();
                    let range_str = ride_stats_map.get("ride_distance").and_then(|v| v.as_s().ok()).unwrap();
                    let range: f64 = range_str.parse().unwrap_or(0.0);
                    curr_range += range;
                    curr_range_yearly += range;
                }
            }
        }

        if in_trip && curr_range > max_range {
            max_range = curr_range;
        }
        if in_trip && curr_range_yearly > max_range_yearly {
            max_range_yearly = curr_range_yearly;
        }
        let key = (imei.to_string(), current_month.clone());
        let value: &mut f64 = imei_month_range.entry(key).or_insert(0.0);
        if max_range > *value {
            *value = max_range;
        }
        let key = imei.to_string();
        let value: &mut f64 = imei_yearly_range.entry(key).or_insert(0.0);
        if max_range_yearly > *value {
            *value = max_range_yearly;
        }
    }

    for ((imei, ride_month), max_range) in imei_month_range.iter() {
        client.put_item()
            .table_name("ride_data_monthly_range")
            .item("imei", AttributeValue::S(imei.clone()))
            .item("date", AttributeValue::S(ride_month.clone()))
            .item("max_range", AttributeValue::N(max_range.to_string()))
            .send()
            .await?;
    } 

    let output: Vec<CustomOutput> = imei_month_range.into_iter().map(|((imei, ride_month), total_range)| {
        CustomOutput {
            imei,
            ride_month,
            total_range,
        }
    }).collect();
    
    Ok(json!(output))
}

async fn query_for_range(
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
