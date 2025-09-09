import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class AppConfig:
    # Flask
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Quickbase
    QB_REALMID: str = os.environ["QB_REALMID"]
    QB_TABLEID: str = os.environ["QB_TABLEID"]
    QB_USER_TOKEN: str = os.environ["QB_USER_TOKEN"]
    PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", "1000"))

    # GCP / Auth
    PROJECT_ID: str = os.environ["PROJECT_ID"]
    BQ_LOCATION: str = os.getenv("BQ_LOCATION", "US")
    GCP_CREDENTIALS_PATH: str = os.environ["GCP_CREDENTIALS_PATH"]

    # GCS
    BUCKET: str = os.environ["BUCKET"]
    GCS_PREFIX: str = os.getenv("GCS_PREFIX", "quickbase_exports/")
    COMPRESS_JSONL: bool = os.getenv("COMPRESS_JSONL", "true").lower() == "true"

    # BQ tables
    BQ_STAGING_DATASET: str = os.environ["BQ_STAGING_DATASET"]
    BQ_STAGING_TABLE: str = os.environ["BQ_STAGING_TABLE"]

    BQ_WORK_DATASET: str = os.environ["BQ_WORK_DATASET"]
    BQ_WORK_TABLE: str = os.environ["BQ_WORK_TABLE"]

    BQ_CORE_DATASET: str = os.environ["BQ_CORE_DATASET"]
    BQ_CORE_TABLE: str = os.environ["BQ_CORE_TABLE"]

    MERGE_KEYS: list[str] = tuple(k.strip() for k in os.getenv("MERGE_KEYS", "id").split(",") if k.strip())
