"""
BigQuery writer service to load data from GCS files into BigQuery tables
"""

import json
import logging
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
import uuid
from datetime import datetime, timezone, timedelta
from schemas.field_mappings import QuickBaseSchema

class BQWriter:
    def __init__(self, project_id: str, credentials_path: str, location: str = 'US'):
        self.project_id = project_id
        self.location = location
        
        # Initialize BigQuery and Storage clients
        creds = service_account.Credentials.from_service_account_file(credentials_path)
        self.bq_client = bigquery.Client(project=project_id, credentials=creds, location=location)
        self.storage_client = storage.Client(project=project_id, credentials=creds)
        
        self.logger = logging.getLogger(__name__)
    
    def create_dataset_if_not_exists(self, dataset_id: str) -> None:
        """Create BigQuery dataset if it doesn't exist"""
        dataset_ref = self.bq_client.dataset(dataset_id)
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            self.logger.info(f'Dataset {dataset_id} already exists')
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            dataset = self.bq_client.create_dataset(dataset)
            self.logger.info(f'Created dataset {dataset_id}')
    
    def _create_temp_table(self, dataset_id: str, table_id: str, schema_fields: List) -> str:
        """Create a temp table that auto-expires (cleanup safety net)."""
        table_ref = self.bq_client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema_fields)
        table.expires = datetime.now(timezone.utc) + timedelta(hours=24)  # auto-cleanup
        table = self.bq_client.create_table(table)
        return f"{self.project_id}.{dataset_id}.{table_id}"

    def load_gcs_file_to_staging_upsert(
        self,
        gcs_uri: str,
        dataset_id: str,
        table_id: str,
        *,
        key_column: str = "record_id",
        modified_ts_column: str = "modified_date",
    ) -> Dict[str, Any]:
        """
        Upsert rows from the GCS file into the staging table by record_id.
        Steps:
        - Ensure staging exists (no drop).
        - Load transformed JSONL into a fresh temp table.
        - MERGE temp -> staging (update if newer, insert if missing).
        - Delete temp in finally; temp also auto-expires.
        """
        temp_fqid = None
        try:
            # Ensure staging exists (do NOT drop)
            schema_fields = QuickBaseSchema.get_bigquery_schema()
            staging_fqid = self.create_table_with_schema(
                dataset_id, table_id, schema_fields, drop_existing=False
            )

            # Create fresh temp
            temp_table = f"{table_id}__load_{uuid.uuid4().hex[:8]}"
            temp_fqid  = self._create_temp_table(dataset_id, temp_table, schema_fields)

            # Transform from GCS
            records = self._read_and_transform_gcs_file(gcs_uri)
            if not records:
                return {
                    "success": True,
                    "message": "No records to upsert",
                    "rows_loaded": 0,
                    "rows_affected": 0,
                    "table_id": staging_fqid,
                }

            # Load into temp
            table_ref = self.bq_client.get_table(temp_fqid)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                max_bad_records=10,
            )
            jsonl_data = "\n".join(json.dumps(r) for r in records)
            load_job = self.bq_client.load_table_from_file(
                StringIO(jsonl_data),
                destination=table_ref,
                job_config=job_config,
            )
            load_job.result()
            if load_job.errors:
                return {
                    "success": False,
                    "message": f"Load to temp failed: {load_job.errors}",
                    "rows_loaded": 0,
                }

            # Dynamic MERGE (dedupe latest per key in temp)
            cols = [m.bq_column_name for m in QuickBaseSchema.FIELD_MAPPINGS]
            update_cols = [c for c in cols if c != key_column]
            set_clause   = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
            insert_cols  = ", ".join([f"`{c}`" for c in cols])
            insert_vals  = ", ".join([f"source.{c}" for c in cols])

            project = self.project_id
            src = f"`{project}.{dataset_id}.{temp_table}`"
            tgt = f"`{project}.{dataset_id}.{table_id}`"

            merge_sql = f"""
            MERGE {tgt} AS target
            USING (
            SELECT * EXCEPT(rn) FROM (
                SELECT s.*, ROW_NUMBER() OVER (
                PARTITION BY {key_column}
                ORDER BY {modified_ts_column} DESC
                ) AS rn
                FROM {src} AS s
            )
            WHERE rn = 1
            ) AS source
            ON target.{key_column} = source.{key_column}
            WHEN MATCHED AND source.{modified_ts_column} > target.{modified_ts_column} THEN
            UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
            """

            merge_job = self.bq_client.query(merge_sql)
            merge_job.result()
            if merge_job.errors:
                return {
                    "success": False,
                    "message": f"MERGE failed: {merge_job.errors}",
                    "rows_loaded": len(records),
                    "table_id": staging_fqid,
                }

            affected = getattr(merge_job, "num_dml_affected_rows", None)
            return {
                "success": True,
                "message": f"Upserted into {staging_fqid}",
                "rows_loaded": len(records),   # loaded into temp
                "rows_affected": affected,     # changed in staging
                "table_id": staging_fqid,
            }

        except Exception as e:
            self.logger.error(f"Upsert to staging failed: {str(e)}")
            return {"success": False, "message": str(e), "rows_loaded": 0}
        finally:
            # Best-effort temp cleanup
            if temp_fqid:
                try:
                    self.bq_client.delete_table(temp_fqid, not_found_ok=True)
                except Exception:
                    self.logger.warning(
                        f"Could not delete temp table {temp_fqid} (it will auto-expire)."
                    )

    def create_table_with_schema(
        self,
        dataset_id: str,
        table_id: str,
        schema_fields: List,
        drop_existing: bool = False
    ) -> str:
        """Create (or recreate) BigQuery table with specified schema."""
        self.create_dataset_if_not_exists(dataset_id)
        table_ref = self.bq_client.dataset(dataset_id).table(table_id)

        def _new_table():
            table = bigquery.Table(table_ref, schema=schema_fields)
            # Partition by modified_date (DAY); cluster by record_id
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="modified_date"
            )
            table.clustering_fields = ["record_id"]
            return self.bq_client.create_table(table)

        if drop_existing:
            try:
                self.bq_client.delete_table(table_ref, not_found_ok=True)
                self.logger.info(f'Dropped existing table {dataset_id}.{table_id}')
            except Exception as e:
                self.logger.info(f'No existing table to drop: {e}')
            _new_table()
            self.logger.info(f'Created fresh table {dataset_id}.{table_id} (partitioned by modified_date, clustered by record_id)')
        else:
            try:
                self.bq_client.get_table(table_ref)
                self.logger.info(f'Table {dataset_id}.{table_id} already exists')
            except Exception:
                _new_table()
                self.logger.info(f'Created table {dataset_id}.{table_id} (partitioned by modified_date, clustered by record_id)')

        return f'{self.project_id}.{dataset_id}.{table_id}'

    def load_gcs_file_to_staging(self, gcs_uri: str, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Load GCS file to BigQuery staging table"""
        try:
            # Create staging table with QuickBase schema
            schema_fields = QuickBaseSchema.get_bigquery_schema()
            self.logger.info(f'Creating table with {len(schema_fields)} schema fields')
            
            full_table_id = self.create_table_with_schema(dataset_id, table_id, schema_fields)
            
            # Read and transform data from GCS
            transformed_records = self._read_and_transform_gcs_file(gcs_uri)
            
            if not transformed_records:
                return {'success': False, 'message': 'No records to load', 'rows_loaded': 0}
            
            # Log first transformed record for debugging
            self.logger.info(f'First transformed record keys: {list(transformed_records[0].keys())}')
            
            # Load transformed data to BigQuery
            table_ref = self.bq_client.get_table(full_table_id)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                max_bad_records=10  # Allow some bad records for debugging
            )
            
            # Convert records to JSONL format
            jsonl_data = '\n'.join([json.dumps(record) for record in transformed_records])
            
            # Load data using StringIO
            job = self.bq_client.load_table_from_file(
                StringIO(jsonl_data),
                destination=table_ref,
                job_config=job_config
            )
            
            job.result()  # Wait for job to complete
            
            if job.errors:
                self.logger.error(f'BigQuery load job errors: {job.errors}')
                return {'success': False, 'message': f'Load job failed: {job.errors}', 'rows_loaded': 0}
            
            rows_loaded = len(transformed_records)
            self.logger.info(f'Loaded {rows_loaded} rows to {full_table_id}')
            
            return {
                'success': True, 
                'message': f'Successfully loaded {rows_loaded} rows',
                'rows_loaded': rows_loaded,
                'table_id': full_table_id
            }
            
        except Exception as e:
            self.logger.error(f'Error loading to BigQuery: {str(e)}')
            return {'success': False, 'message': str(e), 'rows_loaded': 0}
    
    def _read_and_transform_gcs_file(self, gcs_uri: str) -> List[Dict[str, Any]]:
        """Read GCS file and transform QuickBase records to BigQuery format"""
        if not gcs_uri.startswith('gs://'):
            raise ValueError(f'Invalid GCS URI: {gcs_uri}')
        
        # Parse GCS URI
        uri_parts = gcs_uri[5:].split('/', 1)
        bucket_name = uri_parts[0]
        blob_name = uri_parts[1]
        
        # Read file from GCS
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            raise ValueError(f'File not found in GCS: {gcs_uri}')
        
        content = blob.download_as_text()
        self.logger.info(f'Downloaded {len(content)} characters from GCS')
        
        # Parse JSONL content and transform each record
        transformed_records = []
        lines = content.strip().split('\n')
        self.logger.info(f'Processing {len(lines)} lines from GCS file')
        
        for i, line in enumerate(lines):
            if line.strip():
                try:
                    qb_record = json.loads(line)
                    
                    # Debug: Log the first record structure
                    if i == 0:
                        self.logger.info(f"Sample QB record fields: {list(qb_record.keys())}")
                        self.logger.info(f"Sample QB record: {json.dumps(qb_record, indent=2)[:500]}...")
                    
                    transformed_record = QuickBaseSchema.transform_quickbase_record(qb_record)
                    
                    # Debug: Log the first transformed record
                    if i == 0:
                        self.logger.info(f"Sample transformed record keys: {list(transformed_record.keys()) if transformed_record else 'None'}")
                        self.logger.info(f"Sample transformed record: {json.dumps(transformed_record, indent=2)[:500]}...")
                    
                    if transformed_record:
                        # Ensure all expected fields are present (set to None if missing)
                        expected_fields = [mapping.bq_column_name for mapping in QuickBaseSchema.FIELD_MAPPINGS]
                        for field in expected_fields:
                            if field not in transformed_record:
                                transformed_record[field] = None
                        
                        transformed_records.append(transformed_record)
                        
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error on line {i}: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"Transform error on line {i}: {e}")
                    continue
        
        self.logger.info(f"Successfully transformed {len(transformed_records)} records")
        return transformed_records
    
    def transfer_staging_to_work_table(self, staging_dataset: str, staging_table: str, 
                                     work_dataset: str, work_table: str) -> Dict[str, Any]:
        """Transfer data from staging table to work table"""
        try:
            # Create work table if it doesn't exist
            schema_fields = QuickBaseSchema.get_bigquery_schema()
            work_table_id = self.create_table_with_schema(work_dataset, work_table, schema_fields)
            
            # SQL query to transfer data
            query = f"INSERT INTO `{self.project_id}.{work_dataset}.{work_table}` SELECT * FROM `{self.project_id}.{staging_dataset}.{staging_table}`"
            
            job_config = bigquery.QueryJobConfig()
            job = self.bq_client.query(query, job_config=job_config)
            job.result()
            
            if job.errors:
                return {'success': False, 'message': f'Transfer failed: {job.errors}'}
            
            return {
                'success': True,
                'message': f'Successfully transferred data to {work_table_id}',
                'rows_transferred': job.num_dml_affected_rows
            }
            
        except Exception as e:
            self.logger.error(f'Error transferring to work table: {str(e)}')
            return {'success': False, 'message': str(e)}