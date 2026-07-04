import inspect
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scheduler.scheduler import JobScheduler


@pytest.fixture
def sched(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    mock_logger = MagicMock()
    with patch.object(JobScheduler, "_setup_logging", return_value=mock_logger), \
         patch.object(JobScheduler, "_init_file_watcher"):
        return JobScheduler(config_dir=str(config_dir))


# --- Duplication check ---

def test_single_run_script_definition():
    """Exactly one _run_script method in JobScheduler."""
    source = inspect.getsource(JobScheduler)
    assert source.count("def _run_script") == 1


# --- cwd behaviour ---

def test_default_cwd_is_script_parent(sched, tmp_path):
    """Without working_dir, cwd defaults to script_path.parent."""
    script = tmp_path / "scripts" / "job.py"
    script.parent.mkdir()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    assert Path(kwargs["cwd"]) == script.parent


def test_working_dir_overrides_cwd(sched, tmp_path):
    """working_dir parameter is used as cwd instead of script_path.parent."""
    script = tmp_path / "scripts" / "job.py"
    script.parent.mkdir()
    working_dir = tmp_path / "project_root"
    working_dir.mkdir()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None, working_dir=str(working_dir))

    _, kwargs = mock_run.call_args
    assert Path(kwargs["cwd"]) == working_dir


# --- PYTHONPATH behaviour ---

def test_pythonpath_contains_effective_cwd(sched, tmp_path):
    """subprocess.run receives env with PYTHONPATH containing the effective working dir."""
    script = tmp_path / "scripts" / "job.py"
    script.parent.mkdir()

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    assert "env" in kwargs
    parts = kwargs["env"].get("PYTHONPATH", "").split(os.pathsep)
    assert str(script.parent) in parts


def test_existing_pythonpath_preserved(sched, tmp_path):
    """Existing PYTHONPATH entries are kept when working_dir is prepended."""
    script = tmp_path / "scripts" / "job.py"
    script.parent.mkdir()
    existing = "/some/existing/path"

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {"PYTHONPATH": existing}):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    parts = kwargs["env"]["PYTHONPATH"].split(os.pathsep)
    assert str(script.parent) in parts
    assert existing in parts


def test_no_duplicate_pythonpath_entry(sched, tmp_path):
    """working_dir already in PYTHONPATH is not duplicated."""
    script = tmp_path / "scripts" / "job.py"
    script.parent.mkdir()
    cwd_str = str(script.parent)

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {"PYTHONPATH": cwd_str}):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    parts = kwargs["env"]["PYTHONPATH"].split(os.pathsep)
    assert parts.count(cwd_str) == 1


# --- Project root detection ---

def test_pythonpath_contains_project_root_via_git(sched, tmp_path):
    """Project root (containing .git) is added to PYTHONPATH when no working_dir set."""
    # Mimics: atlassian_utils/.git  +  atlassian_utils/SKVU_US/us_health_monitor.py
    project_root = tmp_path / "atlassian_utils"
    (project_root / ".git").mkdir(parents=True)
    script_dir = project_root / "SKVU_US"
    script_dir.mkdir()
    script = script_dir / "us_health_monitor.py"

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    parts = kwargs["env"].get("PYTHONPATH", "").split(os.pathsep)
    assert str(project_root) in parts


def test_project_root_detected_with_relative_script_and_working_dir(sched, tmp_path):
    """With relative script_path + working_dir, root detection resolves under working_dir."""
    project_root = tmp_path / "myproject"
    project_root.mkdir()
    (project_root / "requirements.txt").touch()
    (project_root / "subpkg").mkdir()

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script("subpkg/myscript.py", None, working_dir=str(project_root))

    _, kwargs = mock_run.call_args
    parts = kwargs["env"].get("PYTHONPATH", "").split(os.pathsep)
    assert str(project_root) in parts


def test_no_parent_added_when_no_marker_found(sched, tmp_path):
    """When no project marker exists, PYTHONPATH only contains effective_cwd."""
    # tmp_path has no .git / pyproject.toml etc.
    script = tmp_path / "orphan" / "script.py"
    script.parent.mkdir()

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    parts = kwargs["env"].get("PYTHONPATH", "").split(os.pathsep)
    # Only the script's parent (effective_cwd), nothing else
    assert parts == [str(script.parent)]


def test_project_root_not_duplicated_when_equals_effective_cwd(sched, tmp_path):
    """If project root == effective_cwd, it appears only once in PYTHONPATH."""
    project_root = tmp_path / "proj"
    project_root.mkdir()
    (project_root / "pyproject.toml").touch()
    script = project_root / "main.py"

    with patch("subprocess.run") as mock_run, \
         patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        sched._run_script(str(script), None)

    _, kwargs = mock_run.call_args
    parts = kwargs["env"].get("PYTHONPATH", "").split(os.pathsep)
    assert parts.count(str(project_root)) == 1
