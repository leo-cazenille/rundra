from __future__ import annotations

import re

_SSH_DESTINATION = re.compile(
    r"(?:[A-Za-z0-9][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9_.-]*\Z"
)


def is_credential_field(field: str) -> bool:
    """Recognize credential-bearing field names that must never be persisted."""
    normalized = field.casefold().replace("-", "_")
    return normalized in {
        "password",
        "passphrase",
        "api_key",
        "access_key",
        "authorization",
        "auth_header",
        "private_key",
        "secret",
        "secret_key",
        "token",
        "credential",
        "credentials",
    } or normalized.endswith(
        (
            "_password",
            "_secret",
            "_token",
            "_api_key",
            "_access_key",
            "_secret_key",
            "_credential",
            "_credentials",
        )
    )


def is_safe_ssh_destination(value: str) -> bool:
    """Return whether a host alias or user@host is one unambiguous SSH token."""
    return _SSH_DESTINATION.fullmatch(value) is not None
