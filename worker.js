// worker.js
import * as duckdb from 'npm:@duckdb/duckdb-wasm';

self.onmessage = async (event) => {
  const { bundle } = event.data;
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, self);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  self.postMessage({ status: 'initialized' });
};

