#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Embeddings Migration Pipeline
============================

**ANONYMIZED VERSION FOR PORTFOLIO/EDUCATIONAL USE**

This is a sanitized version of a production Airflow DAG with all company-specific
information removed or replaced with generic placeholders.

Migrates embeddings data from MongoDB to Snowflake using Parquet format.
- Processes embeddings by source with incremental timestamp tracking
- Exports to cloud storage as Parquet files, then merges into Snowflake
- Maintains state file for efficient incremental processing
- Cleans up temporary Parquet files after successful processing

Schedule: Daily at 8 AM
"""

import os
import json
import pendulum
from datetime import datetime, timedelta
from contextlib import closing
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from text_unidecode import unidecode

from airflow.decorators import task, dag
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DESTINATION_FOLDER = "export-embeddings-to-sf"

# Environment configuration - all values from Airflow Variables
SNOWFLAKE_ENV = Variable.get('snowflake_env', default_var='dev')

# Dynamic environment-based configuration
snowflake_conn_id = Variable.get(f'snowflake_{SNOWFLAKE_ENV}_conn_id', default_var=f'snowflake-{SNOWFLAKE_ENV}-app-creds')
snowflake_storage_integration = Variable.get(f'snowflake_{SNOWFLAKE_ENV}_storage_integration', default_var=f'{SNOWFLAKE_ENV}-storage-integration')

# Snowflake database/schema
SNOWFLAKE_DATABASE = Variable.get('snowflake_database', default_var='ANALYTICS_DB')
SNOWFLAKE_SCHEMA = Variable.get('snowflake_schema', default_var='MAIN')
TABLE_NAME = Variable.get('embeddings_table_name', default_var='EMBEDDINGS')

# MongoDB configuration
MONGO_CONN_ID = Variable.get('mongo_conn_id', default_var='mongo-source')
MONGO_DATABASE = Variable.get('mongo_database', default_var='source_db')
MONGO_COLLECTION = Variable.get('mongo_collection', default_var='embeddings')

# GCS configuration
GCS_CONN_ID = Variable.get('gcs_conn_id', default_var='cloud-storage-conn')

local_tz = pendulum.timezone("UTC")

def mimic_utf8_general_ci(data: str) -> str:
    """
    Convert to ASCII string and format for normalized comparison searches.
    This mimics database collation normalization patterns.

    :param data: str - data string to format.
    :return: str - data converted to ASCII string with right
                   blank space trimmed and lowercased.
    """
    return unidecode(str(data)).replace(".", "").replace("$", "").rstrip().lower()

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    default_args=default_args,
    schedule="0 8 * * *",
    description="Moves embeddings data from MongoDB to Snowflake using Parquet format.",
    tags=["embeddings-pipeline", "parquet"],
    max_active_runs=1,
    catchup=False,
)
def embeddings_pipeline():

    @task
    def get_distinct_sources_from_embeddings() -> List:
        """Get all sources that have embeddings data to process."""
        mongo_hook: MongoHook = MongoHook(conn_id=MONGO_CONN_ID)
        collection = mongo_hook.get_collection(
            mongo_collection=MONGO_COLLECTION, mongo_db=MONGO_DATABASE
        )
        return collection.distinct("source")

    @task
    def get_last_processed_timestamps() -> Dict:
        """Load last processed timestamps per source from GCS state file."""
        try:
            gcs_hook: GCSHook = GCSHook(gcp_conn_id=GCS_CONN_ID)
            state_file = f"{DESTINATION_FOLDER}/embeddings_last_processed_timestamp.json"

            if gcs_hook.exists(bucket_name=snowflake_storage_integration, object_name=state_file):
                content = gcs_hook.download(
                    bucket_name=snowflake_storage_integration,
                    object_name=state_file
                )
                data = json.loads(content.decode('utf-8'))

                # Convert ISO strings to datetime objects - JSON can't store datetimes, but MongoDB queries need them
                timestamps = {}
                for source, iso_timestamp in data.items():
                    timestamps[source] = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
                return timestamps
            else:
                print("No timestamp file found, will process all available data")
                return {}
        except Exception as e:
            print(f"Error reading timestamp markers: {e}")
            return {}

    @task(pool="mongo_processing_pool", retries=2)
    def process_embeddings_by_source(source: str, last_timestamps: Dict, **context):
        """Extract embeddings for a source from MongoDB and export as Parquet to GCS."""
        mongo_hook: MongoHook = MongoHook(conn_id=MONGO_CONN_ID)
        gcs_hook: GCSHook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        client = gcs_hook.get_conn()

        bucket = client.bucket(snowflake_storage_integration)
        blob = bucket.blob(
            f'{DESTINATION_FOLDER}/embeddings_{context["ts_nodash"]}_{source}.parquet'
        )

        # Build incremental query
        query = {
            'source': source,
            'embeddings': {'$ne': None, '$exists': True, '$not': {'$size': 0}}
        }
        since_timestamp = last_timestamps.get(source)

        if since_timestamp:
            query['timestamp'] = {'$gt': since_timestamp}
            print(f"Incremental query for {source} since: {since_timestamp.isoformat()}")
        else:
            fallback_date_str = Variable.get('embeddings_fallback_date', default_var='2024-01-01')
            fallback_date = datetime.fromisoformat(fallback_date_str)
            query['timestamp'] = {'$gte': fallback_date}
            print(f"First run for {source} - processing embeddings from {fallback_date.isoformat()} onwards")

        latest_timestamp = since_timestamp
        records_processed = 0
        all_records = []

        try:
            # Process in smaller batches to avoid cursor timeouts
            batch_size = int(Variable.get('embeddings_batch_size', default_var='1000'))
            skip = 0

            while True:
                cur = mongo_hook.find(
                    mongo_collection=MONGO_COLLECTION,
                    query=query,
                    projection={"_id": 0},
                    no_cursor_timeout=True,
                    limit=batch_size,
                    skip=skip,
                )

                batch_records = []
                batch_count = 0

                for doc in cur:
                    try:
                        # Extract and validate core fields
                        cleaned_ref = doc.get('cleaned_ref', '')
                        embedding = doc.get('embeddings', [])
                        country = doc.get('country', [])

                        if not embedding:
                            continue

                        # Track latest timestamp
                        if 'timestamp' in doc and doc['timestamp']:
                            if not latest_timestamp or doc['timestamp'] > latest_timestamp:
                                latest_timestamp = doc['timestamp']

                        # Compute derived fields
                        color = doc.get('color', '')
                        main_refco = f"{cleaned_ref}_{color}" if color else cleaned_ref

                        # Apply character normalization for consistent matching
                        main_refco = mimic_utf8_general_ci(main_refco)

                        # Ensure main_refco doesn't exceed length limit
                        max_length = int(Variable.get('main_refco_max_length', default_var='100'))
                        if len(main_refco) > max_length:
                            main_refco = main_refco[:max_length]

                        # Generate display name
                        country_code = None
                        if isinstance(country, list) and country:
                            country_code = str(country[0])
                        elif isinstance(country, str) and country:
                            country_code = country

                        display_name = f"{source} ({country_code})"

                        # Build record
                        record = {
                            "cleaned_ref": cleaned_ref,
                            "category": doc.get('category', ''),
                            "main_refco": main_refco,
                            "display_name": display_name,
                            "embeddings_type": doc.get('embeddings_type'),
                            "for_matching": doc.get('for_matching', False),
                            "embedding_vector": embedding,
                            "original_timestamp": doc['timestamp'].isoformat(),
                        }

                        batch_records.append(record)
                        batch_count += 1

                    except Exception as e:
                        print(f"Error processing document: {e}")
                        continue

                # Add batch to all_records
                all_records.extend(batch_records)
                records_processed += batch_count

                print(f"Processed batch of {batch_count} records for {source} (total: {records_processed})")

                if batch_count < batch_size:
                    break

                skip += batch_size

            # Save as Parquet
            if all_records:
                print(f"Converting {len(all_records)} records to Parquet for {source}")
                df = pd.DataFrame(all_records)
                table = pa.Table.from_pandas(df)

                with blob.open("wb") as f:
                    pq.write_table(table, f, compression='snappy')

                print(f"Successfully uploaded {blob.name} with {len(all_records)} records")
            else:
                print(f"No valid embeddings found for {source}")

        except Exception as e:
            print(f"Error processing {source}: {e}")
            if blob.exists():
                blob.delete()
            raise e

        return {
            "blob_name": blob.name if all_records else None,
            "source": source,
            "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None,
            "records_processed": records_processed,
            "unique_records": len(all_records) if all_records else 0,
        }

    @task(pool="snowflake_merge_pool", retries=3)
    def merge_into_snowflake(result: Dict):
        """Merge Parquet data from GCS into Snowflake embeddings table."""
        if result["records_processed"] == 0:
            print(f"No records to process for {result['source']}")
            return result

        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        blob_name = result["blob_name"]
        filename = blob_name.split("/")[-1]

        full_table_path = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}"
        sources_config_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{Variable.get('sources_config_table', default_var='sources_config')}"

        # Get configurable stage and file format names
        stage_name = Variable.get('snowflake_stage_name', default_var='EXPORT_EMBEDDINGS_TO_SF_STG')
        file_format = Variable.get('snowflake_file_format', default_var='parquet_embeddings_format')

        # Execute MERGE operation
        merge_sql = f"""
        MERGE INTO {full_table_path} AS target
        USING (
            SELECT 
                stage_data.cleaned_ref,
                stage_data.category,
                stage_data.main_refco,
                stage_data.display_name,
                sc.display_name_id,
                stage_data.embeddings_type,
                stage_data.for_matching,
                stage_data.embedding_vector,
                stage_data.original_timestamp,
                stage_data.embedding_inserted_at
            FROM (
                SELECT 
                    $1:cleaned_ref::STRING AS cleaned_ref,
                    $1:category::STRING AS category,
                    $1:main_refco::STRING AS main_refco,
                    $1:display_name::STRING AS display_name,
                    $1:embeddings_type::STRING AS embeddings_type,
                    $1:for_matching::BOOLEAN AS for_matching,
                    $1:embedding_vector::VECTOR(FLOAT, 128) AS embedding_vector,
                    $1:original_timestamp::STRING AS original_timestamp,
                    CURRENT_TIMESTAMP() AS embedding_inserted_at
                FROM @{stage_name} (
                    file_format => {file_format},
                    pattern => '.*{filename}'
                )
                QUALIFY ROW_NUMBER() OVER (PARTITION BY $1:main_refco::STRING ORDER BY $1:original_timestamp::STRING DESC) = 1
            ) stage_data
            INNER JOIN {sources_config_table} sc 
                ON stage_data.display_name = sc.display_name
        ) AS source
        ON target.main_refco = source.main_refco
        WHEN MATCHED THEN
            UPDATE SET
                target.category = source.category,
                target.display_name = source.display_name,
                target.display_name_id = source.display_name_id,
                target.embeddings_type = source.embeddings_type,
                target.for_matching = source.for_matching,
                target.embedding_vector = source.embedding_vector,
                target.original_timestamp = source.original_timestamp,
                target.embedding_inserted_at = source.embedding_inserted_at
        WHEN NOT MATCHED THEN
            INSERT (
                cleaned_ref, category, main_refco,
                display_name, display_name_id,
                embeddings_type, for_matching, embedding_vector, original_timestamp,
                embedding_inserted_at
            )
            VALUES (
                source.cleaned_ref, source.category, source.main_refco,
                source.display_name, source.display_name_id,
                source.embeddings_type, source.for_matching, source.embedding_vector,
                source.original_timestamp, source.embedding_inserted_at
            )
        """

        try:
            with closing(snowflake_hook.get_conn()) as conn:
                with snowflake_hook._get_cursor(conn, return_dictionaries=False) as cur:
                    cur.execute(merge_sql)
                    rows_affected = cur.rowcount
                    print(f"Merged {rows_affected} rows for source {result['source']}")
                    return result
        except Exception as e:
            print(f"Error merging data for {result['source']}: {e}")
            raise e

    @task
    def collect_timestamps(results: List[Dict]):
        """Extract latest timestamps from processing results for state tracking."""
        new_timestamps = {}
        for result in results:
            if result["latest_timestamp"]:
                new_timestamps[result["source"]] = datetime.fromisoformat(result["latest_timestamp"])
        return new_timestamps

    @task
    def update_processed_timestamps(timestamps: Dict):
        """Save updated timestamps to GCS state file for next incremental run."""
        try:
            gcs_hook: GCSHook = GCSHook(gcp_conn_id=GCS_CONN_ID)

            # Convert datetime objects to ISO strings for JSON serialization
            iso_timestamps = {}
            for source, timestamp in timestamps.items():
                iso_timestamps[source] = timestamp.isoformat()

            # Upload to GCS
            state_file = f"{DESTINATION_FOLDER}/embeddings_last_processed_timestamp.json"
            gcs_hook.upload(
                bucket_name=snowflake_storage_integration,
                object_name=state_file,
                data=json.dumps(iso_timestamps, indent=2).encode('utf-8')
            )

            print(f"Updated timestamp markers: {len(timestamps)} sources")
        except Exception as e:
            print(f"Error updating timestamp markers: {e}")

    @task
    def log_file_metrics(results: List[Dict], **context):
        """Log pipeline metrics for monitoring and optimization."""
        gcs_hook: GCSHook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        client = gcs_hook.get_conn()

        total_size = 0
        total_records = 0
        files_processed = 0

        for result in results:
            if result["records_processed"] > 0 and result.get("blob_name"):
                blob_name = result["blob_name"]
                try:
                    bucket = client.bucket(snowflake_storage_integration)
                    blob = bucket.blob(blob_name)
                    blob.reload()
                    file_size = blob.size

                    total_size += file_size
                    total_records += result["records_processed"]
                    files_processed += 1

                    size_mb = file_size / (1024 * 1024)
                    print(f"File: {result['source']} - {size_mb:.2f} MB, {result['records_processed']} records")

                except Exception as e:
                    print(f"Error getting metrics for {blob_name}: {e}")

        total_size_mb = total_size / (1024 * 1024)
        print(f"""
Pipeline Summary:
- Files processed: {files_processed}
- Total size: {total_size_mb:.2f} MB  
- Total records: {total_records:,}
        """)

    @task
    def cleanup_parquet_files(results: List[Dict]):
        """Delete temporary Parquet files from GCS after successful processing."""
        gcs_hook: GCSHook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        client = gcs_hook.get_conn()
        bucket = client.bucket(snowflake_storage_integration)

        files_deleted = 0

        for result in results:
            if result.get("blob_name") and result["records_processed"] > 0:
                blob_name = result["blob_name"]
                try:
                    blob = bucket.blob(blob_name)
                    blob.delete()
                    files_deleted += 1
                    print(f"Deleted: {blob_name}")

                except Exception as e:
                    print(f"Error deleting file {blob_name}: {e}")
                    # Don't raise - cleanup failures shouldn't break the pipeline

        print(f"Cleanup complete: {files_deleted} files deleted")

        return {"files_deleted": files_deleted}

    # Pipeline execution flow - setup tasks run in parallel
    distinct_sources = get_distinct_sources_from_embeddings()
    last_timestamps = get_last_processed_timestamps()

    # Process embeddings by source in parallel (waits for both setup tasks)
    processing_results = process_embeddings_by_source.partial(last_timestamps=last_timestamps).expand(
        source=distinct_sources
    )

    # Merge results into Snowflake
    merge_results = merge_into_snowflake.partial().expand(result=processing_results)

    # Update state and log metrics
    collected_timestamps = collect_timestamps(merge_results)
    timestamp_update = update_processed_timestamps(collected_timestamps)
    metrics_logging = log_file_metrics(merge_results)

    # Clean up temporary files after successful processing
    cleanup_task = cleanup_parquet_files(merge_results)

    merge_results >> collected_timestamps >> timestamp_update
    merge_results >> metrics_logging >> cleanup_task   # Cleanup runs after merge completes


embeddings_pipeline()