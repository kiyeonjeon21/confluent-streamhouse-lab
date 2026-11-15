"""Keeps credentials out of logs, tracebacks, and terminal output."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field

MASK = "********"


@dataclass(frozen=True)
class Secret:
    """A credential that prints as a mask.

    The value is reachable only through `reveal()`, so an accidental f-string,
    repr in a traceback, or dataclass dump never shows it.
    """

    _value: str = field(repr=False)

    def reveal(self) -> str:
        return self._value

    def __str__(self) -> str:
        return MASK

    def __repr__(self) -> str:
        return f"Secret({MASK})"

    def __bool__(self) -> bool:
        return bool(self._value)


@dataclass(frozen=True)
class ApiKey:
    """An API key ID and its secret. The ID is not sensitive on its own, the secret is."""

    key: str
    secret: Secret

    def basic_token(self) -> Secret:
        raw = f"{self.key}:{self.secret.reveal()}".encode()
        return Secret(base64.b64encode(raw).decode())

    def __repr__(self) -> str:
        return f"ApiKey(key={self.key!r}, secret={MASK})"


def scrub(text: str, *secrets: Secret | ApiKey | None) -> str:
    """Replace every occurrence of the given secrets in text, for subprocess output."""
    for item in secrets:
        if item is None:
            continue
        values = (
            [item.secret.reveal(), item.basic_token().reveal()]
            if isinstance(item, ApiKey)
            else [item.reveal()]
        )
        for value in values:
            if value:
                text = text.replace(value, MASK)
    return text
