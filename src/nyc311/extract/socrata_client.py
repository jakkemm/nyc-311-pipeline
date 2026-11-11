import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from nyc311 import config


def raw_path(day: date) -> Path:
    return config.RAW_DIR / f"{day.isoformat()}.jsonl"

def fetch_day(day: date) -> Path:
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    where = (
        f"created_date >= '{start.isoformat()}' "
        f"AND created_date < '{end.isoformat()}'"
    )
    
    headers = {"X-App-Token": config.SOCRATA_APP_TOKEN}
    out_path = raw_path(day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".tmp") 
    
    total, offset = 0, 0
    
    with open(tmp_path, "w", encoding="utf-8") as f:
        while True:
            params = {
                "$where": where,
                "$order": "unique_key",
                "$offset": offset,
                "$limit": config.PAGE_SIZE,
            }
            resp = requests.get(config.SOCRATA_URL, params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            rows = resp.json()

            if not rows:
                break

            f.writelines([json.dumps(r) + "\n" for r in rows])

            total += len(rows)
            offset += config.PAGE_SIZE
            if len(rows) < config.PAGE_SIZE:
                break

    tmp_path.replace(out_path)
    print(f"{day}: wrote {total} rows to {out_path}")
    return out_path


if __name__ == "__main__":
    fetch_day(date.fromisoformat(sys.argv[1]))