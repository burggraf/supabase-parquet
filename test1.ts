import * as duckdb from "https://deno.land/x/duckdb@0.1.1/mod.ts";
// import duckdb from "duckdb";
// Open a DuckDB database
//const db = open("/tmp/test.db");
// const db = open(":memory:");
console.log("create handle to db...");
const db = new duckdb.Database(":memory:"); 

// Create a connection
const connection = db.connect();

// Example query
const query = "SELECT 42 AS fortytwo";
const result = connection.query(query);
console.log(result);
