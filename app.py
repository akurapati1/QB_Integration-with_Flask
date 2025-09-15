# app.py
from flask import Flask, jsonify, request
import logging
import os

from config import AppConfig
from services.quickbase_client import QuickbaseClient
from services.gcs_writer import GCSWriter
from services.bq_writer import BQWriter

def create_app():
    app = Flask(__name__)
    cfg = AppConfig()

    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    log = logging.getLogger("qb_flask")

    qb = QuickbaseClient(cfg.QB_REALMID, cfg.QB_USER_TOKEN, cfg.QB_TABLEID)
    gcs = GCSWriter(cfg.PROJECT_ID, cfg.GCP_CREDENTIALS_PATH, cfg.BUCKET, cfg.GCS_PREFIX, cfg.COMPRESS_JSONL)

    

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    @app.route("/run", methods=["GET", "POST"])
    def run():
        """Complete automated pipeline: QuickBase -> GCS -> BigQuery"""
        try:
            # Step 1: Extract from QuickBase and write to GCS
            object_name = gcs.new_object_name()
            uri = f"gs://{gcs.bucket_name}/{object_name}"
            log.info(f"Starting pipeline - Target GCS object: {uri}")

            page_size = int(cfg.PAGE_SIZE)
            skip = 0
            total_rows = 0
            pages = 0

            # Extract from QuickBase to GCS
            with gcs.open_jsonl(object_name) as writer:
                while True:
                    page = qb.get_records(page_size=page_size, skip=skip, flatten_values=False)
                    if not page:
                        break

                    n = gcs.write_batch(writer, page)
                    total_rows += n
                    pages += 1
                    log.info(f"Appended page #{pages} (skip={skip}, rows={n})")
                    skip += page_size

            log.info(f"GCS upload complete. Pages={pages}, Rows={total_rows}, GCS={uri}")
            
            # Step 2: Load from GCS to BigQuery
            log.info(f"Starting BigQuery load from: {uri}")
            
            # Initialize BQ Writer
            bq_writer = BQWriter(cfg.PROJECT_ID, cfg.GCP_CREDENTIALS_PATH, cfg.BQ_LOCATION)
            
            # Load to staging table
            staging_result = bq_writer.load_gcs_file_to_staging(
                gcs_uri=uri,
                dataset_id=cfg.BQ_STAGING_DATASET,
                table_id=cfg.BQ_STAGING_TABLE
            )
            
            if not staging_result['success']:
                log.error(f"BigQuery load failed: {staging_result['message']}")
                return jsonify({
                    "ok": False, 
                    "error": f"BigQuery load failed: {staging_result['message']}",
                    "gcs_uri": uri,
                    "gcs_rows": total_rows
                }), 500
            
            log.info(f"Pipeline complete! GCS: {total_rows} rows, BigQuery: {staging_result['rows_loaded']} rows")
            
            return jsonify({
                "ok": True,
                "gcs_uri": uri,
                "gcs_pages": pages,
                "gcs_rows": total_rows,
                "bigquery_result": staging_result,
                "bigquery_table": staging_result['table_id'],
                "message": f"Pipeline complete: {total_rows} rows processed"
            })

        except Exception as e:
            log.exception("Complete pipeline failed")
            return jsonify({"ok": False, "error": str(e)}), 500
        
    return app