"""Unit tests for ecoli-sources server-side sync hardening.

Tests the helper functions added in the hardening pass:
- _parse_github_owner_repo (org allowlist, URL validation)
- _validate_manifest (manifest existence, required columns)
- _safe_s3_key (path traversal rejection)
- _upload_source_tree (skip dirs/extensions, file count limit)
- _sync_ecoli_sources_from_github (backend guard)
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from viva_api.common.handlers.simulations import (
    _ALLOWED_SOURCE_ORGS,
    _MAX_FILE_COUNT,
    _SKIP_DIRS,
    _SKIP_EXTENSIONS,
    _parse_github_owner_repo,
    _safe_s3_key,
    _sync_ecoli_sources_from_github,
    _upload_source_tree,
    _validate_manifest,
)
from viva_api.config import ComputeBackend

# ---------------------------------------------------------------------------
# _parse_github_owner_repo
# ---------------------------------------------------------------------------


class TestParseGithubOwnerRepo:
    def test_valid_allowed_org(self) -> None:
        owner, repo, full = _parse_github_owner_repo("https://github.com/vivarium-collective/ecoli-sources")
        assert owner == "vivarium-collective"
        assert repo == "ecoli-sources"
        assert full == "vivarium-collective/ecoli-sources"

    def test_valid_with_trailing_slash(self) -> None:
        owner, repo, full = _parse_github_owner_repo("https://github.com/CovertLab/some-repo/")
        assert owner == "CovertLab"
        assert repo == "some-repo"

    def test_valid_with_dot_git_suffix(self) -> None:
        owner, repo, _ = _parse_github_owner_repo("https://github.com/CovertLabEcoli/ecoli-sources.git")
        assert owner == "CovertLabEcoli"
        assert repo == "ecoli-sources"

    def test_all_allowed_orgs_accepted(self) -> None:
        for org in _ALLOWED_SOURCE_ORGS:
            owner, _, _ = _parse_github_owner_repo(f"https://github.com/{org}/test-repo")
            assert owner == org

    def test_disallowed_org_raises_403(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _parse_github_owner_repo("https://github.com/evil-org/malicious-repo")
        assert exc_info.value.status_code == 403
        assert "evil-org" in exc_info.value.detail

    def test_invalid_url_no_github(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _parse_github_owner_repo("https://gitlab.com/vivarium-collective/repo")
        assert exc_info.value.status_code == 400

    def test_invalid_url_too_short(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _parse_github_owner_repo("not-a-url")
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# _validate_manifest
# ---------------------------------------------------------------------------


class TestValidateManifest:
    def test_valid_manifest(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        manifest = data_dir / "manifest.tsv"
        manifest.write_text("dataset_id\tfile_path\textra_col\nds1\t/some/path\tfoo\n")
        _validate_manifest(str(tmp_path))  # should not raise

    def test_missing_manifest_raises_400(self, tmp_path: Path) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _validate_manifest(str(tmp_path))
        assert exc_info.value.status_code == 400
        assert "missing data/manifest.tsv" in exc_info.value.detail

    def test_empty_manifest_raises_400(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "manifest.tsv").write_text("")
        with pytest.raises(HTTPException) as exc_info:
            _validate_manifest(str(tmp_path))
        assert exc_info.value.status_code == 400
        assert "empty" in exc_info.value.detail

    def test_missing_required_columns_raises_400(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "manifest.tsv").write_text("dataset_id\tsome_other_col\n")
        with pytest.raises(HTTPException) as exc_info:
            _validate_manifest(str(tmp_path))
        assert exc_info.value.status_code == 400
        assert "file_path" in exc_info.value.detail


# ---------------------------------------------------------------------------
# _safe_s3_key
# ---------------------------------------------------------------------------


class TestSafeS3Key:
    def test_normal_path(self) -> None:
        result = _safe_s3_key("sources/repo/main", "data/manifest.tsv")
        assert result == "sources/repo/main/data/manifest.tsv"

    def test_traversal_rejected(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _safe_s3_key("sources/repo/main", "../../etc/passwd")
        assert exc_info.value.status_code == 400
        assert "traversal" in exc_info.value.detail.lower()

    def test_dot_dot_in_middle_rejected(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _safe_s3_key("prefix", "a/b/../../../secret")
        assert exc_info.value.status_code == 400

    def test_single_dot_allowed(self) -> None:
        result = _safe_s3_key("prefix", "./data/file.txt")
        assert "data/file.txt" in result


# ---------------------------------------------------------------------------
# _upload_source_tree
# ---------------------------------------------------------------------------


class TestUploadSourceTree:
    def test_uploads_normal_files(self, tmp_path: Path) -> None:
        (tmp_path / "file1.txt").write_text("hello")
        (tmp_path / "file2.py").write_text("print('hi')")
        s3 = MagicMock()
        count = _upload_source_tree(s3, "bucket", "prefix", str(tmp_path))
        assert count == 2
        assert s3.upload_file.call_count == 2

    def test_skips_excluded_dirs(self, tmp_path: Path) -> None:
        for skip_dir in _SKIP_DIRS:
            d = tmp_path / skip_dir
            d.mkdir(exist_ok=True)
            (d / "should_skip.txt").write_text("skip me")
        (tmp_path / "keep.txt").write_text("keep me")
        s3 = MagicMock()
        count = _upload_source_tree(s3, "bucket", "prefix", str(tmp_path))
        assert count == 1
        uploaded_path = s3.upload_file.call_args[0][0]
        assert "keep.txt" in uploaded_path

    def test_skips_excluded_extensions(self, tmp_path: Path) -> None:
        (tmp_path / "good.py").write_text("ok")
        for ext in _SKIP_EXTENSIONS:
            (tmp_path / f"bad{ext}").write_bytes(b"\x00")
        s3 = MagicMock()
        count = _upload_source_tree(s3, "bucket", "prefix", str(tmp_path))
        assert count == 1

    def test_file_count_limit(self, tmp_path: Path) -> None:
        # Create MAX_FILE_COUNT + 1 files
        for i in range(_MAX_FILE_COUNT + 1):
            (tmp_path / f"file_{i}.txt").write_text(f"{i}")
        s3 = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            _upload_source_tree(s3, "bucket", "prefix", str(tmp_path))
        assert exc_info.value.status_code == 413

    def test_nested_skip_dir(self, tmp_path: Path) -> None:
        nested = tmp_path / "src" / "__pycache__"
        nested.mkdir(parents=True)
        (nested / "cached.pyc").write_bytes(b"\x00")
        (tmp_path / "src" / "main.py").write_text("ok")
        s3 = MagicMock()
        count = _upload_source_tree(s3, "bucket", "prefix", str(tmp_path))
        assert count == 1


# ---------------------------------------------------------------------------
# _sync_ecoli_sources_from_github — backend guard
# ---------------------------------------------------------------------------


class TestSyncEcoliSourcesBackendGuard:
    @pytest.mark.asyncio
    async def test_slurm_backend_raises_400(self) -> None:
        with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.SLURM):
            with pytest.raises(HTTPException) as exc_info:
                await _sync_ecoli_sources_from_github(
                    repo_url="https://github.com/vivarium-collective/ecoli-sources",
                    ref="main",
                    settings=MagicMock(),
                )
            assert exc_info.value.status_code == 400
            assert "K8s/Batch" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_disallowed_org_raises_403_before_download(self) -> None:
        with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
            with pytest.raises(HTTPException) as exc_info:
                await _sync_ecoli_sources_from_github(
                    repo_url="https://github.com/evil-org/bad-repo",
                    ref="main",
                    settings=MagicMock(),
                )
            assert exc_info.value.status_code == 403
