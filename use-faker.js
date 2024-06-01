import duckdb from 'duckdb';

// import * as duckdb from 'duckdb';

// Set up DuckDB connection
const db = new duckdb.Database(':memory:');
// console.log("db:")
// for (let attr in db) {
//     console.log(attr,typeof db[attr]);
// }
// console.log("duckdb:")
// for (let attr in duckdb) {
//     console.log(attr,typeof duckdb[attr]);
// }

// Load the httpfs extension
db.exec("INSTALL httpfs;");
db.exec("LOAD httpfs;");

// Set up S3 credentials
db.exec(`
CREATE SECRET (
  TYPE S3,
  KEY_ID 'xxxxxxxxx',
  SECRET 'xxxxxxxxx',
  ENDPOINT 'xxxxxxxx'
);
`);

// Generate data using Faker in DuckDB
db.register_udf(
  'generate_person',
  'object',
  (seed) => {
    const fake = new Faker({ seed });
    const person = {
      name: fake.name.findName(),
      city: fake.address.city(),
      state: fake.address.stateAbbr(),
      zipCode: fake.address.zipCode(),
      country: fake.address.country(),
      email: fake.internet.email(),
      job: fake.name.jobTitle(),
      company: fake.company.companyName(),
      ssn: fake.datatype.ssn(),
      birthdate: fake.date.birthdate().toISOString().split('T')[0],
      phoneNumber: fake.phone.phoneNumber()
    };
    return person;
  }//,
//   [duckdb.types.DOUBLE],
//   duckdb.types.STRUCT({
//     name: duckdb.types.VARCHAR,
//     city: duckdb.types.VARCHAR,
//     state: duckdb.types.VARCHAR,
//     zipCode: duckdb.types.VARCHAR,
//     country: duckdb.types.VARCHAR,
//     email: duckdb.types.VARCHAR,
//     job: duckdb.types.VARCHAR,
//     company: duckdb.types.VARCHAR,
//     ssn: duckdb.types.VARCHAR,
//     birthdate: duckdb.types.DATE,
//     phoneNumber: duckdb.types.VARCHAR
//   })
);

// Write data to S3 with partitioning and compression
console.log('writing to s3...')
const x1 = await db.exec(`
    SELECT person.* FROM (
        SELECT generate_person(random()) AS person
        FROM range(1, 100) -- Adjust the number of records as needed
`);
console.log('x1:',x1)

db.exec(`
COPY (
    SELECT person.* FROM (
        SELECT generate_person(random()) AS person
        FROM range(1, 100) -- Adjust the number of records as needed
    )
) TO 's3://la/logs/persons'
(FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION ZSTD);
`);

// Close the DuckDB connection
console.log('closing db...')
db.close();
