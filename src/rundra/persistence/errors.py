class RunStoreError(RuntimeError):
    """Base class for actionable persistence failures."""


class RunAlreadyExistsError(RunStoreError):
    """Raised when creating a Run whose identifier is already persisted."""


class RunNotFoundError(RunStoreError):
    """Raised when a requested Run has no persisted record."""


class RunRecordFormatError(RunStoreError):
    """Raised when persisted RunRecord JSON is malformed or unsupported."""
