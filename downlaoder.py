from pathlib import Path
from datetime import datetime
from qfieldcloud_client import get_client
from config import CONFIG
from qfieldcloud_sdk.sdk import FileTransferType
import fiona

def download_latest_gpkg() -> Path:
    client = get_client()
    project_id = CONFIG["qfieldcloud"]["project_id"]

    jobs = client.list_jobs(project_id)
    if not jobs:
        raise RuntimeError("No jobs found")

    finished_jobs = []
    for job in jobs:
        status = client.job_status(job["id"])
        if status.get("status") == "finished":
            finished_jobs.append((job, status))

    if not finished_jobs:
        raise RuntimeError("No finished jobs found")

    latest_job, latest_status = max(
        finished_jobs,
        key=lambda pair: datetime.fromisoformat(pair[1]["finished_at"])
    )

    data_dir = Path(CONFIG["pipeline"]["data_dir"])
    raw_dir = data_dir / "raw"
    project_dir = raw_dir / "project"
    project_dir.mkdir(parents=True, exist_ok=True)

    local_path = raw_dir/"palmTree.gpkg"

    client.download_file(
        project_id=project_id,
        download_type = FileTransferType.PROJECT,
        remote_filename="palmTree.gpkg",
        local_filename=local_path,
        show_progress=False,
    )
    if not local_path.exists() or local_path.stat().st_size == 0:
        raise RuntimeError("Downloaded GPKG is empty")

    return local_path