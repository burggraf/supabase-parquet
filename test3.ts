import { open } from "https://deno.land/x/duckdb/mod.ts";

const db = open("./example.db");
// or const db = open(':memory:');

const connection = db.connect();

connection.query("select 1 as number"); // -> [{ number: 1 }]

for (const row of connection.stream("select 42 as number")) {
  row; // -> { number: 42 }
}

const prepared = connection.prepare(
  "select ?::INTEGER as number, ?::VARCHAR as text",
);

prepared.query(1337, "foo"); // -> [{ number: 1337, text: 'foo' }]
prepared.query(null, "bar"); // -> [{ number: null, text: 'bar' }]

connection.close();
db.close();

