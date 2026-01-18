# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = os.getenv("QFIELD_ENV_FILE", "./qfield_pipeline/s.env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

def env(key: str, default=None, required=False):
    value = os.getenv(key, default)
    if required and value is None:
        raise RuntimeError(f"Missing required env var: {key}")
    return value

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = Path(
    env("DATA_DIR", default=str(BASE_DIR / "data"))
)

LOG_DIR = Path(
    env("LOG_DIR", default=str(BASE_DIR / "logs"))
)

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

CONFIG = {
    "qfieldcloud": {
        "base_url": env("QFIELDCLOUD_URL", required=True),
        "token": env("QFIELDCLOUD_TOKEN", required=True),
        "project_id": env("QFIELDCLOUD_PROJECT_ID", required=True),
    },
    "postgis": {
        "host": env("PG_HOST", required=True),
        "port": int(env("PG_PORT", 5432)),
        "db": env("PG_DB", required=True),
        "user": env("PG_USER", required=True),
        "password": env("PG_PASSWORD", required=True),
    },
    "pipeline": {
        "data_dir": str(DATA_DIR),
        "log_dir": str(LOG_DIR),
        "max_features": int(env("MAX_FEATURES", 10000)),
    }
}