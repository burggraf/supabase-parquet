use clap::Parser;
use fake::{Fake};
use fake::faker::address::en::{CityName, StateName, ZipCode};
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::SafeEmail;
use fake::faker::job::en::Title;
use fake::faker::name::en::Name;
use fake::faker::phone_number::en::PhoneNumber;
use fake::faker::chrono::en::Date;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand::Rng;
use duckdb::{Connection, Result};
use chrono::{Utc, Datelike};
use std::env;

#[derive(Debug)]
struct Person {
    name: String,
    city: String,
    state: String,
    zip_code: String,
    country: String,
    email: String,
    job: String,
    company: String,
    ssn: String,
    birthdate: String,
    phone_number: String,
    created_year: i32,
    created_month: i32,
}

fn generate_person(seed: u64, created_year: i32, created_month: i32) -> Person {
    let mut rng = StdRng::seed_from_u64(seed);
    Person {
        name: Name().fake_with_rng::<String, _>(&mut rng),
        city: CityName().fake_with_rng::<String, _>(&mut rng),
        state: StateName().fake_with_rng::<String, _>(&mut rng),
        zip_code: ZipCode().fake_with_rng::<String, _>(&mut rng),
        country: "USA".to_string(),
        email: SafeEmail().fake_with_rng::<String, _>(&mut rng),
        job: Title().fake_with_rng::<String, _>(&mut rng),
        company: CompanyName().fake_with_rng::<String, _>(&mut rng),
        ssn: format!("{:09}", rng.gen_range(0..999_999_999)), // Generate a random SSN
        birthdate: Date().fake_with_rng::<chrono::NaiveDate, _>(&mut rng).to_string(),
        phone_number: PhoneNumber().fake_with_rng::<String, _>(&mut rng),
        created_year,
        created_month,
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Total number of records to generate
    #[arg(short, long, default_value_t = 100)]
    total_records: usize,

    /// Batch size for inserting records
    #[arg(short, long, default_value_t = 10)]
    batch_size: usize,

    /// Created year for the records
    #[arg(short, long, default_value_t = Utc::now().year())]
    created_year: i32,

    /// Created month for the records
    #[arg(short, long, default_value_t = Utc::now().month() as i32)]
    created_month: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Load S3 variables from environment
    let s3_access_key_id = env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let s3_secret_access_key = env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    let s3_endpoint = env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set");

    // Connect to DuckDB
    let conn = Connection::open_in_memory()?;

    // Install and load the httpfs extension
    conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;

    // Set S3 credentials and endpoint
    conn.execute_batch(&format!(
        "SET s3_access_key_id='{}';
         SET s3_secret_access_key='{}';
         SET s3_endpoint='{}';",
        s3_access_key_id, s3_secret_access_key, s3_endpoint
    ))?;

    // Create the table
    conn.execute_batch(
        "CREATE TABLE some_people (
            name VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR,
            country VARCHAR,
            email VARCHAR,
            job VARCHAR,
            company VARCHAR,
            ssn VARCHAR,
            birthdate DATE,
            phone_number VARCHAR,
            created_year INTEGER,
            created_month INTEGER
        )",
    )?;

    // Insert data in batches
    {
        let mut appender = conn.appender("some_people")?;

        for i in 0..args.total_records {
            let person = generate_person(i as u64, args.created_year, args.created_month);
            appender.append_row(&[
                &person.name,
                &person.city,
                &person.state,
                &person.zip_code,
                &person.country,
                &person.email,
                &person.job,
                &person.company,
                &person.ssn,
                &person.birthdate,
                &person.phone_number,
                &person.created_year.to_string(),
                &person.created_month.to_string(),
            ])?;

            // Commit the batch every batch_size rows
            if (i + 1) % args.batch_size == 0 {
                appender.flush()?;
            }
        }

        // Flush any remaining rows
        appender.flush()?;
    } // The appender is dropped here

    // Write data to Parquet files in S3
    /*
COPY (SELECT * FROM read_parquet('data.parquet'))
TO 's3://your-s3-bucket/path/to/partitioned'
(FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION ZSTD);    
     */
    //ZSTD
    conn.execute_batch(&format!(
        "COPY some_people TO 's3://people/partitioned' (
            FORMAT PARQUET, 
            PARTITION_BY (created_year, created_month),
            COMPRESSION GZIP,
            OVERWRITE_OR_IGNORE true
        );"
    ))?;

    // Close the connection explicitly
    match conn.close() {
        Ok(_) => println!("Connection closed successfully."),
        Err((_conn, err)) => {
            eprintln!("Failed to close connection: {:?}", err);
            // Optionally, you can handle the connection object `conn` here if needed
        }
    }

    // Return Ok to indicate successful completion
    Ok(())
}
