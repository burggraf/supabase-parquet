//import duckdb from "duckdb";
//import * as duckdb from "https://deno.land/x/duckdb";
import duckdb from "npm:duckdb";
const db = new duckdb.Database(":memory:"); 


const con = db.connect();

con.all("SELECT 42 AS fortytwo", function (err, res) {
  if (err) {
    console.warn(err);
  }
  console.log(res[0].fortytwo);
})

