"""
Build the Airflow connection 'databricks_default' from the DATABRICKS_* values of .env, so the
credentials are typed once. Run it after editing .env:  python scripts/sync_airflow_connection.py
"""

import json
from pathlib import Path

ENV = Path(__file__).resolve().parents[1] / ".env"
KEY = "AIRFLOW_CONN_DATABRICKS_DEFAULT"


def main() -> None:
    lines = ENV.read_text().splitlines()
    values = dict(line.split("=", 1) for line in lines if "=" in line and not line.lstrip().startswith("#"))
    missing = [k for k in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN") if not values.get(k)]
    if missing:
        raise SystemExit(f"Fill these values in .env first: {', '.join(missing)}")
    conn = {
        "conn_type": "databricks",
        "host": values["DATABRICKS_HOST"].removeprefix("https://"),
        "password": values["DATABRICKS_TOKEN"],
        "extra": {"http_path": values["DATABRICKS_HTTP_PATH"]},
    }
    line = f"{KEY}='{json.dumps(conn)}'"
    lines = [l for l in lines if not l.startswith(f"{KEY}=")] + [line]
    ENV.write_text("\n".join(lines) + "\n")
    print(f"{KEY} written to .env (token not displayed)")


if __name__ == "__main__":
    main()
