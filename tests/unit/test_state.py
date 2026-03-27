"""Tests for StateManager."""

from __future__ import annotations

from pathlib import Path

from drt.state.manager import StateManager, SyncState


def test_get_last_sync_missing(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    assert mgr.get_last_sync("no_such_sync") is None


def test_save_and_get(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    state = SyncState(
        sync_name="my_sync",
        last_run_at="2024-01-01T00:00:00+00:00",
        records_synced=42,
        status="success",
    )
    mgr.save_sync(state)

    loaded = mgr.get_last_sync("my_sync")
    assert loaded is not None
    assert loaded.sync_name == "my_sync"
    assert loaded.records_synced == 42
    assert loaded.status == "success"
    assert loaded.error is None


def test_save_overwrites(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("s", "2024-01-01T00:00:00+00:00", 10, "success"))
    mgr.save_sync(SyncState("s", "2024-01-02T00:00:00+00:00", 20, "partial", error="oops"))

    loaded = mgr.get_last_sync("s")
    assert loaded is not None
    assert loaded.records_synced == 20
    assert loaded.error == "oops"


def test_get_all(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("a", "2024-01-01T00:00:00+00:00", 1, "success"))
    mgr.save_sync(SyncState("b", "2024-01-01T00:00:00+00:00", 2, "failed"))

    all_states = mgr.get_all()
    assert set(all_states.keys()) == {"a", "b"}
    assert all_states["b"].status == "failed"


def test_creates_drt_dir_on_demand(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    assert not (tmp_path / ".drt").exists()

    mgr.save_sync(SyncState("s", "2024-01-01T00:00:00+00:00", 0, "success"))
    assert (tmp_path / ".drt" / "state.json").exists()
