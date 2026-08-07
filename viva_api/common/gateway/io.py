import io
import tempfile
import zipfile
from pathlib import Path

from fastapi import BackgroundTasks


def get_zip_buffer(dirpath: Path) -> io.BytesIO:
    files = list(dirpath.iterdir())
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            if file.exists():
                zipf.write(file, arcname=file.name)
    zip_buffer.seek(0)
    return zip_buffer


def write_zip_buffer(buffer: io.BytesIO, filename: str, background_tasks: BackgroundTasks) -> Path:
    tmpdir = tempfile.TemporaryDirectory()
    background_tasks.add_task(tmpdir.cleanup)
    filepath = Path(tmpdir.name) / f"{filename}.zip"
    with open(filepath, "wb") as f:
        f.write(buffer.getvalue())
    return filepath


def write_zip(dirpath: Path, zip_filename: str, background_tasks: BackgroundTasks) -> Path:
    buffer = get_zip_buffer(dirpath)
    return write_zip_buffer(buffer, zip_filename, background_tasks)
