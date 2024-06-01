-- Load the httpfs extension
INSTALL httpfs;
LOAD httpfs;

-- Set S3 credentials and endpoint from environment variables
SET s3_access_key_id = getenv('S3_ACCESS_KEY_ID');
SET s3_secret_access_key = getenv('S3_SECRET_ACCESS_KEY');
SET s3_endpoint = getenv('S3_ENDPOINT');

-- Perform a simple query on the Parquet files in the S3 bucket
SELECT count(*) as aug FROM read_parquet('s3://people/partitioned/**') where created_year = 2024 and created_month = 8 and state = 'Idaho';

