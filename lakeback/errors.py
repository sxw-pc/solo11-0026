"""Exception hierarchy and stable process exit codes for LakeBack."""

from __future__ import annotations

# Exit codes are part of the command-line contract.
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_RESIDUAL = 4
EXIT_CYCLE = 5


class LakeBackError(Exception):
    """Base class for expected, user-facing failures."""

    exit_code = EXIT_USAGE
    code = "error"

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigError(LakeBackError):
    """Invalid configuration or unavailable backend."""

    code = "config_error"


class BackendError(LakeBackError):
    """Backend-level read, write, or availability failure."""

    code = "backend_error"


class LedgerError(LakeBackError):
    """Corrupt, tampered, or inconsistent snapshot ledger."""

    code = "ledger_error"


class NotFoundError(LakeBackError):
    """Table, snapshot, version, timestamp, or report was not found."""

    code = "not_found"
    exit_code = EXIT_NOT_FOUND


class DataError(LakeBackError):
    """Table data or schema cannot be interpreted."""

    code = "data_error"


class RequestError(LakeBackError):
    """Invalid HTTP request parameters."""

    code = "bad_request"


class ResidualDiff(LakeBackError):
    """Audit completed and found residual differences (CI gate)."""

    code = "residual_diff"
    exit_code = EXIT_RESIDUAL

    def __init__(self, message: str, *, report: dict | None = None) -> None:
        super().__init__(message)
        self.report = report or {}


class LineageCycleError(LakeBackError):
    """A dependency cycle was found while walking the snapshot lineage graph."""

    code = "lineage_cycle"
    exit_code = EXIT_CYCLE

    def __init__(self, message: str, *, cycle: list[str] | None = None) -> None:
        super().__init__(message, details={"cycle": list(cycle or [])})
        self.cycle = list(cycle or [])
