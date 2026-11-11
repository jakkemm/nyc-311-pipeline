import os
from pathlib import Path

# Paths inside the container
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
MARTS_DIR = DATA_DIR / "marts"

# NYC Open Data: 311 Service Requests from 2020 to Present
SOCRATA_URL = "https://nycopendata.socrata.com/resource/erm2-nwe9.json"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN") or None
PAGE_SIZE = 10_000
