// Import the parquetjs library
//import parquetjs from 'npm:@dsnp/parquetjs/dist/browser/parquetjs.esm'; // Adjust the path as necessary
import parquetjs from 'npm:@dsnp/parquetjs'; // Adjust the path as necessary
// Function to write data to a Parquet file
async function writeParquet(filePath: string, schema: any, data: any[]) {
  const writer = await parquetjs.ParquetWriter.openFile(schema, filePath);
  for (const row of data) {
    await writer.appendRow(row);
  }
  await writer.close();
  console.log(`Data written to Parquet file: ${filePath}`);
}

// Function to read a Parquet file
async function readParquet(filePath: string) {
  const reader = await parquetjs.ParquetReader.openFile(filePath);
  const cursor = reader.getCursor();
  let record = null;
  console.log("Read Parquet File:");
  while (record = await cursor.next()) {
    console.log(record);
  }
  await reader.close();
  return cursor;
}

// Function to append data to a Parquet file
async function appendParquet(filePath: string, data: any[]) {

    const existingData = await readParquet(parquetFilePath);
    
    const writer = await parquetjs.ParquetWriter.openFile(existingData.schema, filePath);
    console.log(`Appending data to Parquet file: ${filePath}`);
    console.log('writer.rowCount', writer.envelopeWriter.rowCount)
    const combinedData = existingData.rowGroup.concat(data);

    for (const row of combinedData) {
      await writer.appendRow(row);
    }
    await writer.close();
    console.log(`Data appended to Parquet file: ${filePath}`);
}
  
// Example usage
const parquetFilePath = "./example.parquet";
const schema = new parquetjs.ParquetSchema({
  name: { type: 'UTF8' },
  age: { type: 'INT32' },
  city: { type: 'UTF8' }
});

const data = [
  { name: 'John Doe', age: 30, city: 'New York' },
  { name: 'Jane Doe', age: 25, city: 'Los Angeles' }
];

// Write data to Parquet file
console.log('Writing data to Parquet file...');
await writeParquet(parquetFilePath, schema, data);

// Read Parquet file
console.log('Reading data from Parquet file...');
await readParquet(parquetFilePath);

// Append data to Parquet file
const newData = [
  { name: 'Alice', age: 28, city: 'Chicago' },
  { name: 'Bob', age: 35, city: 'San Francisco' }
];
console.log('Appending data to Parquet file...');
await appendParquet(parquetFilePath, newData);

// Read Parquet file
console.log('2: Reading data from Parquet file...');
await readParquet(parquetFilePath);
