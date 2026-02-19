from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.state_store import MeltanoState
from meltano.core.state_store.base import StateIDLockedError

if TYPE_CHECKING:
    from meltano_state_backend_postgresql.backend import PostgreSQLStateStoreManager

# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------


def test_set_state(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Execute is called with the correct parameters when setting state."""
    manager, mock_cursor = subject

    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)

    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == (
        "test_job",
        json.dumps({"singer_state": {"partial": 1}}),
        json.dumps({"singer_state": {"complete": 1}}),
    )


def test_get_state(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """State is returned and correctly deserialised."""
    manager, mock_cursor = subject

    mock_cursor.fetchone.return_value = (
        '{"singer_state": {"partial": 1}}',
        '{"singer_state": {"complete": 1}}',
    )

    state = manager.get("test_job")
    assert state is not None
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_job",)


@pytest.mark.parametrize(
    ("partial_raw", "completed_raw", "expected_partial", "expected_completed"),
    (
        pytest.param(
            None,
            '{"singer_state": {"complete": 1}}',
            {},
            {"singer_state": {"complete": 1}},
            id="null_partial",
        ),
        pytest.param(None, None, {}, {}, id="both_null"),
    ),
)
def test_get_state_null_columns(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
    partial_raw: str | None,
    completed_raw: str | None,
    expected_partial: dict[str, object],
    expected_completed: dict[str, object],
) -> None:
    """NULL TEXT columns are returned as empty dicts."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = (partial_raw, completed_raw)

    state = manager.get("test_job")
    assert state is not None
    assert state.state_id == "test_job"
    assert state.partial_state == expected_partial
    assert state.completed_state == expected_completed


def test_get_state_not_found(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """None is returned when the state does not exist."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = None

    assert manager.get("nonexistent") is None


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_state(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Execute is called with the correct state_id when deleting."""
    manager, mock_cursor = subject

    manager.delete("test_job")

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_job",)


# ---------------------------------------------------------------------------
# get_state_ids
# ---------------------------------------------------------------------------


def test_get_state_ids(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """All state IDs are returned when no pattern is given."""
    manager, mock_cursor = subject
    mock_cursor.fetchall.return_value = ["job1", "job2", "job3"]

    state_ids = list(manager.get_state_ids())

    mock_cursor.execute.assert_called()
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Glob pattern is converted to SQL LIKE and used in the query."""
    manager, mock_cursor = subject
    mock_cursor.fetchall.return_value = ["test_job_1", "test_job_2"]

    state_ids = list(manager.get_state_ids("test_*"))

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_%",)
    assert state_ids == ["test_job_1", "test_job_2"]


# ---------------------------------------------------------------------------
# clear_all
# ---------------------------------------------------------------------------


def test_clear_all(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """clear_all returns the count returned by the pre-truncate query."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = (5,)

    count = manager.clear_all()

    assert mock_cursor.execute.call_count >= 2
    assert count == 5


# ---------------------------------------------------------------------------
# acquire_lock
# ---------------------------------------------------------------------------


def test_acquire_lock(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Lock is acquired and released without error."""
    manager, mock_cursor = subject

    with manager.acquire_lock("test_job", retry_seconds=0):
        mock_cursor.execute.assert_called()

    assert mock_cursor.execute.call_count >= 3  # table setup + lock + release


def test_acquire_lock_retry(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Lock is retried when the first attempt fails."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.side_effect = [False, True]

    with (
        mock.patch("meltano_state_backend_postgresql.backend.sleep"),
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        pass

    assert mock_cursor.execute.call_count >= 2


def test_acquire_lock_max_retries_exceeded(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """StateIDLockedError is raised when the lock cannot be acquired within the timeout."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = False

    retry_seconds = Decimal("0.01")

    with (  # noqa: SIM117
        mock.patch("meltano_state_backend_postgresql.backend.sleep") as mock_sleep,
        pytest.raises(
            StateIDLockedError,
            match="Could not acquire lock for state_id: test_job",
        ),
    ):
        with manager.acquire_lock("test_job", retry_seconds=retry_seconds):  # type: ignore[arg-type]
            pass  # pragma: no cover

    assert mock_sleep.call_count == int(30 / retry_seconds) - 1


def test_acquire_lock_multiple_retries_then_success(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Lock is acquired after several failed attempts."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.side_effect = [False, False, False, True]

    with (
        mock.patch("meltano_state_backend_postgresql.backend.sleep") as mock_sleep,
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(0.01)
