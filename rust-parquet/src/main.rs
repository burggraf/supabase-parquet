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
}

fn generate_person(seed: u64) -> Person {
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
    }
}

fn main() -> Result<()> {
    // Connect to DuckDB
    let conn = Connection::open("./people.db")?;

    // Create the table
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS people (
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
            phone_number VARCHAR
        )",
    )?;

    // Insert data in batches
    {
        let mut appender = conn.appender("people")?;

        for i in 0..1000000 {
            let person = generate_person(i);
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
            ])?;

            // Commit the batch every 10,000 rows
            if (i + 1) % 10000 == 0 {
                appender.flush()?;
            }
        }

        // Flush any remaining rows
        appender.flush()?;
    } // The appender is dropped here

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