from pathlib import Path

from pydantic import BaseModel


class HPCFilePath(BaseModel):
    """Represents a file path on the HPC remote system.

    Configuration via JSON in .env files:
        HPC_IMAGE_BASE_PATH={"remote_path": "/projects/SMS/api/dev/images"}

    Path translation is configured via settings:
        path_local_prefix: Local mount prefix (e.g., /Volumes/SMS)
        path_remote_prefix: Remote HPC prefix (e.g., /projects/SMS)

    Usage:
        # For SSH commands - use str() or remote_path
        ssh.run_command(f"ls {path}")

        # For local file access - use local_path()
        with open(path.local_path() / "file.txt") as f: ...
    """

    remote_path: Path

    def __str__(self) -> str:
        """Return remote path string (for SSH commands)."""
        return str(self.remote_path)

    def local_path(self) -> Path:
        """Return pathlib.Path for local filesystem access.

        Translates remote_path using path_local_prefix and path_remote_prefix
        from settings.

        Raises:
            ValueError: If path_local_prefix or path_remote_prefix are not configured.

        Returns:
            Path: The translated local path for filesystem access.
        """
        from viva_api.config import get_settings

        settings = get_settings()
        local_prefix = settings.path_local_prefix
        remote_prefix = settings.path_remote_prefix

        if not local_prefix or not remote_prefix:
            raise ValueError(
                "path_local_prefix and path_remote_prefix must be configured in settings "
                "to use local_path(). Set these in your .env file."
            )

        # If prefixes are the same, no translation needed
        if local_prefix == remote_prefix:
            return self.remote_path

        # Translate: swap remote_prefix for local_prefix
        try:
            rel = self.remote_path.relative_to(remote_prefix)
            return Path(local_prefix) / rel
        except ValueError:
            # Path doesn't start with remote_prefix - return as-is
            # This handles cases where the path is already local or uses a different base
            return self.remote_path

    @property
    def parent(self) -> "HPCFilePath":
        return HPCFilePath(remote_path=self.remote_path.parent)

    @property
    def name(self) -> str:
        return self.remote_path.name

    def __truediv__(self, other: str) -> "HPCFilePath":
        if "{" in other or "}" in other or ":" in other or '"' in other:
            raise ValueError(f"HPCFilePath can not composing paths from json strings, unexpected arg '{other}'")
        return HPCFilePath(remote_path=self.remote_path / other)


class ScratchFilePath(BaseModel):
    """Represents a local scratch file path.

    Configuration via JSON in .env files:
        SCRATCH_DIR={"local_path": "/tmp/scratch"}
    """

    local_path: Path

    def __str__(self) -> str:
        return str(self.local_path)

    @property
    def parent(self) -> "ScratchFilePath":
        return ScratchFilePath(local_path=self.local_path.parent)

    @property
    def name(self) -> str:
        return self.local_path.name

    def __truediv__(self, other: str) -> "ScratchFilePath":
        if "{" in other or "}" in other or ":" in other or '"' in other:
            raise ValueError(f"ScratchFilePath can not composing paths from json strings, unexpected arg '{other}'")
        return ScratchFilePath(local_path=self.local_path / other)


class S3FilePath(BaseModel):
    """Represents an S3 file path without the bucket.

    Configuration via JSON in .env files:
        S3_DATA_PATH={"s3_path": "/path"}
    """

    # bucket: str
    s3_path: Path

    def __str__(self) -> str:
        return str(self.s3_path)

    @property
    def parent(self) -> "S3FilePath":
        return S3FilePath(s3_path=self.s3_path.parent)

    @property
    def name(self) -> str:
        return self.s3_path.name

    def __truediv__(self, other: str) -> "S3FilePath":
        if "{" in other or "}" in other or ":" in other or '"' in other:
            raise ValueError(f"S3FilePath can not composing paths from json strings, unexpected arg '{other}'")
        return S3FilePath(s3_path=self.s3_path / other)
