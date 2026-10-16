"""End-to-end tests which parse a real dbt project with whichever dbt-core version is installed."""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from dbt import flags

from dbt_contracts.cli import ValidateCommand

pytest.importorskip("dbt.adapters.postgres", reason="Requires the dbt-postgres adapter to parse the project")

PROJECT_DIR = Path(__file__).parent.joinpath("project")

# (term, item name, parent name) for each violation deliberately left in the sample project
EXPECTED_RESULTS = {
    ("has_description", "orders", None),
    ("has_no_final_semicolon", "orders", None),
    ("has_description", "name", "customers"),
    ("has_description", "payments", None),
    ("has_properties", "cents_to_dollars", None),
}


@pytest.fixture(autouse=True)
def restore_dbt_flags():
    """Invoking dbt replaces its global flags, so restore them to avoid leaking state into other tests."""
    original = flags.get_flags()
    yield
    flags.set_flags(original)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Copy the sample project so dbt writes its artifacts and logs to a temporary directory."""
    path = tmp_path.joinpath("project")
    shutil.copytree(PROJECT_DIR, path)
    return path


def _validate(project_dir: Path) -> set[tuple[str, str, str | None]]:
    args = ValidateCommand.parse(args=["--project-dir", str(project_dir), "--profiles-dir", str(project_dir)])
    runner = args._create_runner()
    return {(result.result_name, result.name, result.parent_name) for result in runner.validate()}


@pytest.mark.integration
def test_validate_from_parsed_and_stored_manifest(project_dir: Path):
    manifest_path = project_dir.joinpath("target", "manifest.json")
    assert not manifest_path.exists()

    # no stored manifest, so dbt parses the project
    assert _validate(project_dir) == EXPECTED_RESULTS
    assert manifest_path.is_file()

    # the manifest written by dbt is now loaded from the target directory
    assert _validate(project_dir) == EXPECTED_RESULTS


def _run_validate_command(project_dir: Path, *args: str) -> subprocess.CompletedProcess:
    """Run the ``dbt-validate`` command as a user would, in a new process."""
    command = "from dbt_contracts.cli import validate; validate()"
    return subprocess.run(
        [sys.executable, "-c", command, "--project-dir", str(project_dir), "--profiles-dir", str(project_dir), *args],
        capture_output=True, text=True,
    )


@pytest.mark.integration
def test_validate_command_fails_on_violations_without_traceback(project_dir: Path):
    result = _run_validate_command(project_dir)
    output = result.stdout + result.stderr

    assert result.returncode == 1
    assert "Traceback" not in output
    assert output.strip().splitlines()[-1].endswith(f"Found {len(EXPECTED_RESULTS)} contract violations\x1b[39m")


@pytest.mark.integration
def test_validate_command_does_not_fail_with_no_fail(project_dir: Path):
    result = _run_validate_command(project_dir, "--no-fail")
    assert result.returncode == 0
    assert "Traceback" not in result.stdout + result.stderr
