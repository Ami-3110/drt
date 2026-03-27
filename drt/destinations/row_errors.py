"""Row-level error tracking for detailed sync reporting.

DetailedSyncResult is backward-compatible with SyncResult:
it has all the same fields plus ``row_errors``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RowError:
    """Error detail for a single record that failed to sync."""

    batch_index: int
    record_preview: str   # First 200 chars — avoids PII logging of full record
    http_status: int | None
    error_message: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


@dataclass
class DetailedSyncResult:
    """Extended SyncResult with per-row error details.

    Backward-compatible: exposes the same ``success``, ``failed``,
    ``skipped``, and ``errors`` attributes as SyncResult.
    """

    success: int = 0
    failed: int = 0
    skipped: int = 0
    row_errors: list[RowError] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped

    @property
    def errors(self) -> list[str]:
        """Backward-compatible: flat list of error messages."""
        return [e.error_message for e in self.row_errors]
