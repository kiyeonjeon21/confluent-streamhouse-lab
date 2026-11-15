"""Guards against committing secrets and against Terraform drifting from fmt/validate."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from streamhouse_lab.config import REPO_ROOT, TERRAFORM_DIR

MUST_IGNORE = [
    ".env",
    "terraform/terraform.tfstate",
    "terraform/terraform.tfstate.backup",
    "terraform/terraform.tfvars",
    "terraform/.terraform/providers",
    "results/run.jsonl",
]
MUST_TRACK = [".env.example", "terraform/terraform.tfvars.example", "terraform/.terraform.lock.hcl"]

SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?secret|password)\s*[:=]\s*['\"]?[A-Za-z0-9+/]{32,}"),
    re.compile(r"\bcflt[A-Za-z0-9+/]{40,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True, check=False)


@pytest.mark.parametrize("path", MUST_IGNORE)
def test_secret_paths_are_ignored(path: str) -> None:
    assert git("check-ignore", "-q", "--no-index", path).returncode == 0, path


@pytest.mark.parametrize("path", MUST_TRACK)
def test_templates_are_not_ignored(path: str) -> None:
    assert git("check-ignore", "-q", "--no-index", path).returncode == 1, path


def candidate_files() -> list[Path]:
    listed = git("ls-files", "--cached", "--others", "--exclude-standard")
    return [REPO_ROOT / line for line in listed.stdout.splitlines() if line]


def test_no_secret_looking_strings_in_tracked_or_new_files() -> None:
    offenders = []
    for path in candidate_files():
        if not path.is_file() or path.suffix in {".png", ".lock"} or path.name == "uv.lock":
            continue
        text = path.read_text(errors="ignore")
        if any(p.search(text) for p in SECRET_PATTERNS):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_terraform_is_formatted() -> None:
    proc = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "fmt", "-check", "-recursive"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout


@pytest.mark.skipif(
    shutil.which("terraform") is None or not (TERRAFORM_DIR / ".terraform").exists(),
    reason="terraform not initialised; run scripts/verify-local",
)
def test_terraform_validates() -> None:
    proc = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "validate", "-no-color"], capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
