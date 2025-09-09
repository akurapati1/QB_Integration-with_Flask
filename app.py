# app.py (Flask version of "call QB API and load to GCS")
from flask import Flask, jsonify
import logging, os

from config import AppConfig
from services.quickbase_client import QuickbaseClient
from services.gcs_writer import GCSWriter

def create_app():
    app = Flask(__name__)
    cfg = AppConfig()

    # logging
    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    log = logging.getLogger("qb_flask")

    # services
    qb  = QuickbaseClient(cfg.QB_REALMID, cfg.QB_USER_TOKEN, cfg.QB_TABLEID, cfg.PAGE_SIZE)
    gcs = GCSWriter(cfg.PROJECT_ID, cfg.GCP_CREDENTIALS_PATH, cfg.BUCKET, cfg.GCS_PREFIX, cfg.COMPRESS_JSONL)

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    @app.route("/run", methods=["GET", "POST"])
    def run():
        try:
            # choose a target object in GCS
            object_name = gcs.new_object_name()
            log.info(f"target GCS object: gs://{gcs.bucket_name}/{object_name}")

            # optional: fetch fields once (iter_batches can rename using this)
            _ = qb.get_field_map()

            # generator over flattened, label-renamed records
            def record_iter():
                for batch in qb.iter_batches(
                    include_fields_each=False,
                    flatten_values=True,
                    rename_to_labels=True,
                    select_all_fields=True,
                ):
                    for rec in batch["records"]:
                        yield rec

            # stream to GCS as JSONL
            rows = gcs.stream_jsonl(object_name, record_iter())
            uri = f"gs://{gcs.bucket_name}/{object_name}"
            log.info(f"uploaded {rows} records to {uri}")
            return jsonify({"ok": True, "gcs_uri": uri, "rows_uploaded": rows})

        except Exception as e:
            log.exception("pipeline failed")
            return jsonify({"ok": False, "error": str(e)}), 500

    return app

# if __name__ == "__main__":
#     app = create_app()
#     app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_ENV") == "development")
