# app.py
from flask import Flask, jsonify
import logging

from config import AppConfig
from services.quickbase_client import QuickbaseClient
from services.gcs_writer import GCSWriter
from services.bq_writer import BQWriter
from utils import gcs_object_epoch_ms

def create_app():
    app = Flask(__name__)
    cfg = AppConfig()
    last_run = None  # in-memory checkpoint (epoch ms)

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
        try:
            # ---------- Step 1: Extract from QuickBase to GCS ----------
            object_name = gcs.new_object_name()
            uri = f"gs://{gcs.bucket_name}/{object_name}"
            log.info(f"Starting pipeline - Target GCS object: {uri}")

            nonlocal last_run
            prev_ms = last_run  # may be None for first run
            where = f"{{2.OAF.{prev_ms}}}" if prev_ms else None
            log.info(f"Quickbase WHERE filter: {where}")

            # Candidate checkpoint for this run (advance only on success)
            current_ms = gcs_object_epoch_ms(object_name)

            page_size = int(cfg.PAGE_SIZE)
            skip = 0
            total_rows = 0
            pages = 0

            with gcs.open_jsonl(object_name) as writer:
                while True:
                    page = qb.get_records(
                        page_size=page_size,
                        skip=skip,
                        flatten_values=False,
                        where=where  # relies on QuickbaseClient supporting 'where'
                    )
                    if not page:
                        break

                    n = gcs.write_batch(writer, page)
                    total_rows += n
                    pages += 1
                    log.info(f"Appended page #{pages} (skip={skip}, rows={n})")
                    skip += page_size

            log.info(f"GCS upload complete. Pages={pages}, Rows={total_rows}, GCS={uri}")

            # ---------- Step 2: Upsert GCS -> BigQuery staging ----------
            log.info(f"Starting BigQuery staging upsert from: {uri}")

            bq_writer = BQWriter(cfg.PROJECT_ID, cfg.GCP_CREDENTIALS_PATH, cfg.BQ_LOCATION)

            staging_result = bq_writer.load_gcs_file_to_staging_upsert(
                gcs_uri=uri,
                dataset_id=cfg.BQ_STAGING_DATASET,
                table_id=cfg.BQ_STAGING_TABLE,
                # key_column="record_id",
                # modified_ts_column="modified_date",
            )

            if not staging_result.get('success'):
                log.error(f"Staging upsert failed: {staging_result.get('message')}")
                return jsonify({
                    "ok": False,
                    "error": f"Staging upsert failed: {staging_result.get('message')}",
                    "gcs_uri": uri,
                    "gcs_pages": pages,
                    "gcs_rows": total_rows
                }), 500

            log.info(
                "Staging upsert complete. Loaded temp rows: %s; rows affected in staging: %s; table: %s",
                staging_result.get('rows_loaded'),
                staging_result.get('rows_affected'),
                staging_result.get('table_id')
            )

            # Advance checkpoint ONLY after successful upsert
            last_run = current_ms

            return jsonify({
                "ok": True,
                "gcs_uri": uri,
                "gcs_pages": pages,
                "gcs_rows": total_rows,
                "staging_result": staging_result,
                "staging_table": staging_result.get("table_id"),
                "last_run_ms": last_run,
                "message": f"Pipeline complete: {total_rows} rows to GCS; upsert affected {staging_result.get('rows_affected')} rows in staging"
            })

        except Exception as e:
            log.exception("Complete pipeline failed")
            return jsonify({"ok": False, "error": str(e)}), 500

    return app
