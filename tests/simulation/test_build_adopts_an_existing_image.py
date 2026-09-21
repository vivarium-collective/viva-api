"""A simulator build ADOPTS an image that already exists; it never rebuilds it (``docs/plan-core.md`` D11).

Dev and prod share one ECR repository and each has its own database. A site with no record of a
commit the other already built used to build it again and push the same tag -- replacing the first
site's image with a different one (seven untagged 5.7 GB originals in the repository by 2026-09-21).
The tags are write-once now (sms-cdk), so that push is refused; adopting is what makes the second
site's request succeed, on the SAME image, and what lets a half-failed build be retried.

The generated script is run FOR REAL here, under ``sh``, with fake ``aws`` / ``docker`` / ``git`` /
``apk`` first on ``PATH`` that record what they were asked to do.
"""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.simulation.test_ray_backend import _ray_settings
from tests.simulation.test_simulators_write_once import _version
from viva_api.simulation.dispatch.build import ImageBuilder

FAKE_GIT = """#!/bin/sh
echo "git $*" >> "$CALLS"
if [ "$1" = clone ]; then
  for last; do :; done
  mkdir -p "$last/docker"
  printf '#!/bin/sh\\necho "recipe $*" >> "$CALLS"\\n' > "$last/docker/build-and-push-ecr.sh"
fi
exit 0
"""

# `aws ecr describe-images ... imageTag=<tag>`: the tag exists if it is listed in $EXISTING;
# with $DENIED set the call fails for another reason altogether.
FAKE_AWS = """#!/bin/sh
echo "aws $*" >> "$CALLS"
case "$*" in
  *describe-images*)
    if [ -n "$DENIED" ]; then
      echo "An error occurred (AccessDeniedException) when calling DescribeImages" >&2; exit 254
    fi
    tag=$(echo "$*" | sed -n "s/.*imageTag=\\([^ ]*\\).*/\\1/p")
    for t in $EXISTING; do [ "$t" = "$tag" ] && { echo "sha256:0123abcd"; exit 0; }; done
    echo "An error occurred (ImageNotFoundException) when calling DescribeImages" >&2; exit 254 ;;
  *get-secret-value*) echo "pat" ;;
  *get-login-password*) echo "password" ;;
esac
exit 0
"""

FAKES = {
    "apk": "#!/bin/sh\nexit 0\n",
    "docker": '#!/bin/sh\necho "docker $*" >> "$CALLS"\nexit 0\n',
    "git": FAKE_GIT,
    "aws": FAKE_AWS,
}


@pytest.fixture
def run_build(tmp_path: Path):  # type: ignore[no-untyped-def]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in FAKES.items():
        (bin_dir / name).write_text(body, encoding="utf-8")
        (bin_dir / name).chmod(0o755)
    calls = tmp_path / "calls.log"

    def _run(
        *, existing: str = "", denied: bool = False, temporary_tag: str | None = None
    ) -> tuple[int, list[str], str]:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = ImageBuilder(local_task_service=None).build_command(  # type: ignore[arg-type]
                _version(temporary_tag), include_submit_image=True
            )[2]
        script = script.replace("/build/v2ecoli", str(tmp_path / "clone"))  # the only path the host cannot give it
        calls.write_text("", encoding="utf-8")
        env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "CALLS": str(calls),
            "EXISTING": existing,
            "DENIED": "1" if denied else "",
            "ECR_REGISTRY": "123.dkr.ecr.us-gov-west-1.amazonaws.com",
            "HOME": str(tmp_path),
        }
        done = subprocess.run(["sh", "-c", script], env=env, capture_output=True, text=True, timeout=60, check=False)  # noqa: S603, S607
        return done.returncode, calls.read_text(encoding="utf-8").splitlines(), done.stdout + done.stderr

    return _run


def _did(calls: list[str], needle: str) -> bool:
    return any(needle in line for line in calls)


def test_a_commit_nobody_built_is_cloned_built_and_pushed_as_before(run_build) -> None:  # type: ignore[no-untyped-def]
    code, calls, _ = run_build()
    assert code == 0
    assert _did(calls, "git clone") and _did(calls, "recipe -i abc1234 ")
    assert _did(calls, "docker build") and _did(calls, "docker push") and _did(calls, ":abc1234-submit")


def test_an_image_another_site_already_built_is_adopted_not_rebuilt(run_build) -> None:  # type: ignore[no-untyped-def]
    code, calls, output = run_build(existing="abc1234 abc1234-submit")
    assert code == 0  # the request SUCCEEDS -- on the same image, which is the point
    adopted = [line for line in output.splitlines() if line.startswith("ADOPTED existing image")]  # not the xtrace echo
    assert len(adopted) == 2 and all("sha256:0123abcd" in line for line in adopted)
    assert not _did(calls, "git clone") and not _did(calls, "recipe ")
    assert not _did(calls, "docker build") and not _did(calls, "docker push")


def test_a_build_that_failed_half_way_is_finished_not_restarted(run_build) -> None:  # type: ignore[no-untyped-def]
    """The main image was pushed and the submit image was not: with write-once tags, rebuilding the
    first would be refused at its push and the simulator could never be completed."""
    code, calls, _ = run_build(existing="abc1234")
    assert code == 0
    assert not _did(calls, "git clone") and not _did(calls, "recipe ")
    assert _did(calls, "docker login")  # the recipe that would have logged docker in did not run
    assert _did(calls, "docker build") and _did(calls, "docker push") and _did(calls, ":abc1234-submit")


def test_not_being_allowed_to_ask_is_said_and_the_build_goes_ahead(run_build) -> None:  # type: ignore[no-untyped-def]
    """A permissions gap must not turn every build into a silent no-op."""
    code, calls, output = run_build(denied=True)
    assert code == 0
    assert "WARNING: could not ask ECR" in output and "AccessDenied" in output
    assert _did(calls, "recipe -i abc1234 ") and _did(calls, "docker push")


def test_a_temporary_simulator_asks_about_its_own_tag_never_the_authoritative_one(run_build) -> None:  # type: ignore[no-untyped-def]
    code, calls, _ = run_build(existing="abc1234 abc1234-submit", temporary_tag="tmp-abc1234-0a1b2c")
    assert code == 0
    asked = [line for line in calls if "describe-images" in line]
    assert asked and all("imageTag=tmp-abc1234-0a1b2c" in line for line in asked)
    assert _did(calls, "recipe -i tmp-abc1234-0a1b2c ")  # the authoritative image existing does not satisfy it
