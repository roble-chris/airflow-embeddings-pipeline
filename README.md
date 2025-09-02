# Airflow Embeddings Pipeline

**ANONYMIZED VERSION FOR PORTFOLIO/EDUCATIONAL USE**

This repository contains a sanitized version of a production Airflow DAG that migrates embeddings data from MongoDB to Snowflake using Parquet format.

## Overview

The pipeline performs the following operations:
- Extracts embeddings data from MongoDB collections
- Processes data incrementally using timestamp tracking
- Exports data to cloud storage as Parquet files
- Merges data into Snowflake tables
- Maintains state files for efficient incremental processing
- Cleans up temporary files after successful processing

**Schedule**: Daily at 8:00 AM

## Features

- ✅ **Incremental Processing**: Only processes new/updated records since last run
- ✅ **State Management**: Maintains processing state for recovery and efficiency
- ✅ **Cloud Storage**: Uses Parquet format for efficient data transfer
- ✅ **Error Handling**: Comprehensive error handling and logging
- ✅ **Cleanup**: Automatic cleanup of temporary files

## Tech Stack

- **Apache Airflow**: Workflow orchestration
- **MongoDB**: Source database for embeddings
- **Snowflake**: Destination data warehouse
- **Google Cloud Storage**: Intermediate storage for Parquet files
- **Python**: Core processing logic with pandas and pyarrow

## Project Structure

```
airflow-embeddings-pipeline/
├── embeddings-pipeline.py    # Main Airflow DAG
├── requirements.txt          # Python dependencies
├── CONFIGURATION.md          # Setup and configuration guide
├── README.md                # This file
└── .gitignore               # Git ignore rules
```

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   - Follow the setup instructions in `CONFIGURATION.md`
   - Set up Airflow Variables and Connections
   - Configure MongoDB and Snowflake access

3. **Deploy**:
   - Copy `embeddings-pipeline.py` to your Airflow DAGs folder
   - Enable the DAG in Airflow UI

## Configuration

See `CONFIGURATION.md` for detailed setup instructions including:
- Required Airflow Variables
- Database connections
- Environment configuration
- Security best practices

## Notes

This is an anonymized version with all company-specific information removed or replaced with generic placeholders. The code structure and logic patterns demonstrate:
- Production-ready Airflow DAG design
- Efficient data pipeline architecture
- Proper error handling and state management
- Cloud-native data processing patterns

## License

This project is for educational and portfolio purposes only.
