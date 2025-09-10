# app.py
from flask import Flask, jsonify
import logging

from config import AppConfig
from services.quickbase_client import QuickbaseClient
from services.gcs_writer import GCSWriter

def create_app():
    app = Flask(__name__)
    cfg = AppConfig()

    # logging.basicConfig(
    #     level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
    #     format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    # )
    # log = logging.getLogger("qb_flask")

    # qb = QuickbaseClient(cfg.QB_REALMID, cfg.QB_USER_TOKEN, cfg.QB_TABLEID)
    # gcs = GCSWriter(cfg.PROJECT_ID, cfg.GCP_CREDENTIALS_PATH, cfg.BUCKET, cfg.GCS_PREFIX, cfg.COMPRESS_JSONL)

    # @app.get("/health")
    # def health():
    #     return jsonify({"ok": True})

    @app.route("/run", methods=["GET", "POST"])
    def run():
        try:
            object_name = gcs.new_object_name()
            uri = f"gs://{gcs.bucket_name}/{object_name}"
            log.info(f"Target GCS object: {uri}")

            page_size = int(cfg.PAGE_SIZE)
            skip = 0
            total_rows = 0
            pages = 0

            # Open once; append each Quickbase page to the same file.
            with gcs.open_jsonl(object_name) as writer:
                while True:
                    page = qb.get_records(page_size=page_size, skip=skip, flatten_values=True)
                    if not page:
                        break

                    n = gcs.write_batch(writer, page)
                    total_rows += n
                    pages += 1
                    log.info(f"Appended page #{pages} (skip={skip}, rows={n})")

                    skip += page_size  # next window

            log.info(f"Done. Pages={pages}, Rows={total_rows}, GCS={uri}")
            return jsonify({"ok": True, "gcs_uri": uri, "pages_uploaded": pages, "rows_uploaded": total_rows})

        except Exception as e:
            log.exception("pipeline failed")
            return jsonify({"ok": False, "error": str(e)}), 500

    return app

# if __name__ == "__main__":
#     app = create_app()
#     app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=(cfg.FLASK_ENV == "development"))
