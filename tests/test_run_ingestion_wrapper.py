"""Tests du wrapper shell airflow/scripts/run_ingestion.sh (subprocess, aucun Airflow requis).

La resolution de l'executable bash passe par shutil.which() plutot que le nom nu
"bash" : sur Windows, plusieurs binaires "bash" coexistent sur le PATH (relai WSL,
Git Bash, ...) et subprocess.run(["bash", ...]) peut resoudre le mauvais (verifie
empiriquement : la recherche interne de subprocess sur Windows a pioche le relai
WSL, qui echoue faute de distribution configuree). shutil.which() renvoie le bon
binaire Git Bash de facon fiable, et reste portable sur un runner Linux/CI.
"""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

WRAPPER_PATH = Path(__file__).resolve().parent.parent / "airflow" / "scripts" / "run_ingestion.sh"
BASH_EXE = shutil.which("bash")

pytestmark = pytest.mark.skipif(BASH_EXE is None, reason="bash introuvable sur le PATH")


def _make_fake_python3(tmp_path: Path, exit_code: int) -> Path:
    """Cree un faux `python3` sur un PATH isole, qui echo ses arguments et sort avec exit_code."""
    fake_bin = tmp_path / "fakebin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3"
    fake_python.write_text(f'#!/usr/bin/env bash\necho "args: $@"\nexit {exit_code}\n')
    fake_python.chmod(fake_python.stat().st_mode | stat.S_IEXEC)
    return fake_bin


def _run_wrapper(
    tmp_path: Path, exit_code: int, args: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    assert BASH_EXE is not None
    fake_bin = _make_fake_python3(tmp_path, exit_code)
    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    return subprocess.run(
        [BASH_EXE, str(WRAPPER_PATH), *(args or [])],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_exit_0_passes_through(tmp_path: Path) -> None:
    result = _run_wrapper(tmp_path, 0)
    assert result.returncode == 0
    assert "WARNING" not in result.stdout


def test_exit_1_is_translated_to_0_with_warning(tmp_path: Path) -> None:
    result = _run_wrapper(tmp_path, 1)
    assert result.returncode == 0
    assert "WARNING" in result.stdout


def test_exit_2_passes_through_unchanged(tmp_path: Path) -> None:
    result = _run_wrapper(tmp_path, 2)
    assert result.returncode == 2
    assert "WARNING" not in result.stdout


def test_arguments_are_forwarded_to_ingestion_run(tmp_path: Path) -> None:
    result = _run_wrapper(tmp_path, 0, args=["--start", "2024-01-01", "--dry-run"])
    assert "args: -m ingestion.run --start 2024-01-01 --dry-run" in result.stdout
