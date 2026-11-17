import gzip
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from nyc311 import config

HEADERS = {"X-App-Token": config.SOCRATA_APP_TOKEN} if config.SOCRATA_APP_TOKEN else None

def _build_filter(day: date) -> str:
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    where = (
        f"created_date >= '{start.isoformat()}' "
        f"AND created_date < '{end.isoformat()}'"
    )
    return where

def fetch_day(day: date) -> Path:
    out_path = config.raw_path(day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".tmp") 
    
    total, offset = 0, 0
    
    with open(tmp_path, "w", encoding="utf-8") as f:
        while True:
            params = {
                "$where": _build_filter(day),
                "$order": "unique_key",
                "$offset": offset,
                "$limit": config.PAGE_SIZE,
            }
            resp = requests.get(config.SOCRATA_URL, params=params, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            rows = resp.json()

            if not rows:
                break

            f.writelines([json.dumps(r) + "\n" for r in rows])

            total += len(rows)
            offset += config.PAGE_SIZE
            if len(rows) < config.PAGE_SIZE:
                break

    with open(tmp_path, "r", encoding="utf-8") as f_in:  # noqa: SIM117
        with gzip.open(out_path, "wt", encoding="utf-8") as f_out:
            f_out.writelines(f_in)
    tmp_path.unlink()

    print(f"{day}: wrote {total} rows to {out_path}")
    return out_path

def is_day_present(day: date) -> bool:
    params = {
        "$select": "count(*)",
        "$where": _build_filter(day),
    }
    resp = requests.get(config.SOCRATA_URL, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    count = int(resp.json()[0]["count"])
    return count > 0

if __name__ == "__main__":
    print(is_day_present(date.fromisoformat(sys.argv[1])))
    fetch_day(date.fromisoformat(sys.argv[1]))
