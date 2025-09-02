# Embeddings Pipeline Configuration

This document outlines all the configuration variables required for the embeddings pipeline DAG.

## Required Airflow Variables

### Environment Configuration
```
snowflake_env = "dev" | "test" | "prod"
```

### Database Configuration
```
# Snowflake
snowflake_database = "YOUR_DATABASE_NAME"
snowflake_schema = "YOUR_SCHEMA_NAME"
embeddings_table_name = "EMBEDDINGS"
sources_config_table = "sources_config"

# MongoDB
mongo_database = "YOUR_MONGO_DB"
mongo_collection = "embeddings"
```

### Connection IDs
```
# Snowflake connection IDs per environment
snowflake_dev_conn_id = "your-dev-snowflake-conn"
snowflake_test_conn_id = "your-test-snowflake-conn" 
snowflake_prod_conn_id = "your-prod-snowflake-conn"

# Storage integrations per environment
snowflake_dev_storage_integration = "your-dev-gcs-bucket"
snowflake_test_storage_integration = "your-test-gcs-bucket"
snowflake_prod_storage_integration = "your-prod-gcs-bucket"

# Other connections
mongo_conn_id = "your-mongo-connection"
gcs_conn_id = "your-gcs-connection"
```

### Snowflake Object Names
```
snowflake_stage_name = "YOUR_STAGE_NAME"
snowflake_file_format = "your_parquet_format"
```

### Processing Configuration
```
embeddings_fallback_date = "2024-08-01"  # ISO format
embeddings_batch_size = "1000"
main_refco_max_length = "100"
```

## Required Airflow Connections

### 1. MongoDB Connection
- **Connection ID**: Value of `mongo_conn_id` variable
- **Connection Type**: MongoDB
- **Host**: Your MongoDB host
- **Schema**: Your MongoDB database
- **Login/Password**: As needed

### 2. Snowflake Connections (per environment)
- **Connection ID**: Values of `snowflake_{env}_conn_id` variables
- **Connection Type**: Snowflake
- **Account**: Your Snowflake account
- **Database**: Your Snowflake database
- **Schema**: Your Snowflake schema
- **Login/Password/Key**: As configured

### 3. Google Cloud Storage Connection
- **Connection ID**: Value of `gcs_conn_id` variable
- **Connection Type**: Google Cloud
- **Keyfile JSON**: Your GCP service account key

## Snowflake Prerequisites

### Required Objects
1. **Stage**: External stage pointing to GCS bucket
2. **File Format**: Parquet format for reading files
3. **Storage Integration**: GCS integration for bucket access
4. **Tables**: 
   - Main embeddings table
   - Sources configuration table

### Example SQL Setup
```sql
-- Create file format
CREATE FILE FORMAT parquet_embeddings_format
TYPE = PARQUET
COMPRESSION = SNAPPY;

-- Create stage
CREATE STAGE EXPORT_EMBEDDINGS_TO_SF_STG
URL = 'gcs://your-bucket/export-embeddings-to-sf/'
STORAGE_INTEGRATION = your_storage_integration
FILE_FORMAT = parquet_embeddings_format;

-- Create embeddings table
CREATE TABLE EMBEDDINGS (
    cleaned_ref STRING,
    category STRING,
    main_refco STRING,
    display_name STRING,
    display_name_id NUMBER,
    embeddings_type STRING,
    for_matching BOOLEAN,
    embedding_vector VECTOR(FLOAT, 128),
    original_timestamp STRING,
    embedding_inserted_at TIMESTAMP
);
```

# Configuration Guide

## Environment Setup

### 1. Python Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Environment Variables
Create a `.env` file in the project root with the following variables:

```bash
# MongoDB Configuration
MONGO_CONNECTION_STRING=mongodb://username:password@host:port/database
MONGO_DATABASE=your_database_name
MONGO_COLLECTION=embeddings_collection

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse

# AWS Configuration (if using S3)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_S3_BUCKET=your_bucket_name

# Airflow Configuration
AIRFLOW_HOME=/path/to/airflow
```

### 3. Airflow Setup
1. Initialize Airflow database: `airflow db init`
2. Create admin user: `airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com`
3. Copy this DAG file to your Airflow DAGs folder
4. Start Airflow: `airflow webserver` and `airflow scheduler`

### 4. Security Notes
- Never commit the `.env` file to version control
- Use Airflow Connections or Variables for production secrets
- Ensure proper network access between Airflow, MongoDB, and Snowflake

## Usage
The pipeline runs daily at 8 AM and processes embeddings incrementally from MongoDB to Snowflake.

## Security Notes

- Store all sensitive values in Airflow Variables/Connections
- Use environment-specific connection IDs
- Rotate service account keys regularly
- Review GCS bucket permissions
- Monitor Snowflake usage and costs