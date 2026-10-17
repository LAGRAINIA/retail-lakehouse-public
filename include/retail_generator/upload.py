"""Upload generated files to a Unity Catalog volume (the landing zone) with the Databricks SDK."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def workspace_client(host: str | None = None, token: str | None = None):
    from databricks.sdk import WorkspaceClient

    host = host or os.environ["DATABRICKS_HOST"]
    if not host.startswith("http"):
        host = f"https://{host}"
    return WorkspaceClient(host=host, token=token or os.environ["DATABRICKS_TOKEN"])


def volume_root(catalog: str | None = None) -> str:
    catalog = catalog or os.getenv("DATABRICKS_CATALOG", "retail")
    return f"/Volumes/{catalog}/landing/raw"


def upload_files(local_root: Path, relative_paths: list[str], volume_path: str, client=None, workers: int = 8) -> int:
    """Upload each file to <volume_path>/<relative path>. Overwrites, so a re-run is idempotent."""
    client = client or workspace_client()
    local_root = Path(local_root)

    def send(rel: str) -> None:
        with (local_root / rel).open("rb") as f:
            client.files.upload(f"{volume_path}/{rel}", f, overwrite=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(send, relative_paths))
    return len(relative_paths)
