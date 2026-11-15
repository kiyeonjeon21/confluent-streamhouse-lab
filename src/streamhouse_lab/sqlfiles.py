"""Discovers the lab's SQL files and the directives in their headers.

Layout, by convention rather than a manifest:

    sql/<NN>-<stage>/<NN>-<name>.sql       applied in file-name order by `lab apply <stage>`
    sql/<NN>-<stage>/checks/<name>.sql     read-only queries run by `lab check <stage>`
    sql/events/<name>.sql                  business events replayed by `lab event <name>`

Each file holds exactly one statement, because the Flink statements API runs
one statement per request. A header comment `-- lab:mode=snapshot` runs the
file as a snapshot (bounded, batch) query; the default is streaming.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from streamhouse_lab.config import SQL_DIR

STATEMENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
STATEMENT_NAME_MAX = 100
MODES = ("streaming", "snapshot")
_DIRECTIVE_RE = re.compile(r"^--\s*lab:(\w+)=(\S+)\s*$")
_STAGE_DIR_RE = re.compile(r"^(\d{2})-([a-z0-9-]+)$")


class SqlFileError(ValueError):
    pass


@dataclass(frozen=True)
class SqlFile:
    path: Path
    statement: str
    mode: str

    @property
    def stem(self) -> str:
        return self.path.stem


@dataclass(frozen=True)
class Stage:
    number: int
    name: str
    path: Path

    def statements(self) -> list[SqlFile]:
        return [parse(p) for p in sorted(self.path.glob("*.sql"))]

    def checks(self) -> list[SqlFile]:
        return [parse(p) for p in sorted((self.path / "checks").glob("*.sql"))]

    def statement_name(self, sql_file: SqlFile) -> str:
        return statement_name("lab", self.name, sql_file.stem)


def statement_name(*parts: str) -> str:
    name = "-".join(parts).lower().replace("_", "-")
    if len(name) > STATEMENT_NAME_MAX or not STATEMENT_NAME_RE.match(name):
        raise SqlFileError(f"invalid Flink statement name: {name!r}")
    return name


def stages(sql_dir: Path = SQL_DIR) -> list[Stage]:
    found = []
    for path in sorted(sql_dir.iterdir()):
        match = _STAGE_DIR_RE.match(path.name)
        if path.is_dir() and match:
            found.append(Stage(int(match.group(1)), match.group(2), path))
    return found


def stage(name: str, sql_dir: Path = SQL_DIR) -> Stage:
    for candidate in stages(sql_dir):
        if name in (candidate.name, candidate.path.name):
            return candidate
    known = ", ".join(s.name for s in stages(sql_dir))
    raise SqlFileError(f"unknown stage {name!r}; known stages: {known}")


def events(sql_dir: Path = SQL_DIR) -> dict[str, SqlFile]:
    return {p.stem: parse(p) for p in sorted((sql_dir / "events").glob("*.sql"))}


def parse(path: Path) -> SqlFile:
    text = path.read_text()
    directives = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("--"):
            break
        match = _DIRECTIVE_RE.match(stripped)
        if match:
            directives[match.group(1)] = match.group(2)

    unknown = set(directives) - {"mode"}
    if unknown:
        raise SqlFileError(f"{path}: unknown directive(s) {sorted(unknown)}")
    mode = directives.get("mode", "streaming")
    if mode not in MODES:
        raise SqlFileError(f"{path}: mode must be one of {MODES}, got {mode!r}")

    statements = split_statements(text)
    if len(statements) != 1:
        raise SqlFileError(f"{path}: expected exactly one statement, found {len(statements)}")
    return SqlFile(path=path, statement=statements[0], mode=mode)


def split_statements(text: str) -> list[str]:
    """Split on top-level semicolons, ignoring those in strings, identifiers, and comments."""
    statements: list[str] = []
    current: list[str] = []
    i, n = 0, len(text)
    quote: str | None = None
    while i < n:
        ch = text[i]
        if quote:
            current.append(ch)
            if ch == quote:
                # A doubled quote is an escaped quote, not the end of the literal.
                if i + 1 < n and text[i + 1] == quote:
                    current.append(text[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", "`", '"'):
            quote = ch
            current.append(ch)
        elif text.startswith("--", i):
            end = text.find("\n", i)
            i = n if end == -1 else end
            continue
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        elif ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements
