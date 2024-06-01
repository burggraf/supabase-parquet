import { faker } from '@faker-js/faker';
import duckdb from 'duckdb';

// Define the function to generate a person's details with a seed parameter
function generatePerson(seed) {
    faker.seed(seed);
    return {
        birthdate: faker.date.birthdate().toISOString().split('T')[0],
        city: faker.location.city(),
        company: faker.company.name(),
        country: faker.location.country(),
        email: faker.internet.email(),
        job: faker.person.jobTitle(),
        name: faker.person.fullName(),
        phone_number: faker.phone.number(),
        ssn: faker.finance.accountNumber(),
        state: faker.location.state(),
        zip_code: faker.location.zipCode()
    };
}

// Function to escape single quotes in strings
function escapeString(str) {
    return str.replace(/'/g, "''");
}

// Generate data
const data = [];
for (let i = 0; i < 10; i++) {
    const person = generatePerson(i);
    for (let key in person) {
        if (typeof person[key] === 'string') {
            person[key] = escapeString(person[key]);
        }
    }
    data.push(person);
}

// Connect to DuckDB
const db = new duckdb.Database(':memory:');

db.run(`
    CREATE TABLE people (
        birthdate DATE,
        city VARCHAR,
        company VARCHAR,
        country VARCHAR,
        email VARCHAR,
        job VARCHAR,
        name VARCHAR,
        phone_number VARCHAR,
        ssn VARCHAR,
        state VARCHAR,
        zip_code VARCHAR
    )
`, (err) => {
    if (err) {
        console.error('Error creating table:', err);
    } else {
        console.log('Table created successfully');

        // Insert data into the table
        const insertRow = (person) => {
            return new Promise((resolve, reject) => {
                const query = `
                    INSERT INTO people (birthdate, city, company, country, email, job, name, phone_number, ssn, state, zip_code)
                    VALUES ('${person.birthdate}', '${person.city}', '${person.company}', '${person.country}', '${person.email}', 
                            '${person.job}', '${person.name}', '${person.phone_number}', '${person.ssn}', '${person.state}', '${person.zip_code}')
                `;
                console.log('Executing query:', query);  // Log the query being executed
                db.run(query, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        };

        // Insert each person one by one
        const insertAllData = async () => {
            try {
                for (const person of data) {
                    await insertRow(person);
                }
                console.log('Data inserted successfully');

                // Query the table
                db.all('SELECT * FROM people', (err, rows) => {
                    if (err) {
                        console.error('Error querying table:', err);
                    } else {
                        console.log('Query results:', rows);
                    }
                });
            } catch (error) {
                console.error('Error inserting data:', error);
            }
        };

        insertAllData();
    }
});
