import * as duckdb from 'npm:@duckdb/duckdb-wasm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

// Create a data URL for the worker script
const workerScript = `
  import * as duckdb from 'npm:@duckdb/duckdb-wasm';
  self.onmessage = async (event) => {
    const { bundle } = event.data;
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, self);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    self.postMessage({ status: 'initialized' });
  };
`;

const blob = new Blob([workerScript], { type: 'application/javascript' });
const worker_url = URL.createObjectURL(blob);

// Instantiate the worker
const worker = new Worker(worker_url, { type: 'module' });

// Send the bundle to the worker
worker.postMessage({ bundle });

// Listen for messages from the worker
worker.onmessage = (event) => {
  if (event.data.status === 'initialized') {
    console.log('DuckDB-Wasm initialized in worker');
  }
};

// Clean up the worker URL if necessary
URL.revokeObjectURL(worker_url);

