"""Source Protocol — the interface all sources must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
Future PyO3 bindings will implement this same protocol.
"""

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from drt.config.credentials import ProfileConfig


@runtime_checkable
class Source(Protocol):
    """Extract records from a data warehouse or database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:  # type: ignore[empty-body]
        """Yield records one at a time from the source."""
        ...

    def test_connection(self, config: ProfileConfig) -> bool:  # type: ignore[empty-body]
        """Return True if the source is reachable."""
        ...
