from __future__ import annotations

import json
import os
import platform
import shutil
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.project import Project
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from meltano.core.state_store.base import (
    MissingStateBackendSettingsError,
    StateIDLockedError,
)
from testcontainers.postgres import PostgresContainer

from meltano_state_backend_postgresql.backend import PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:16", driver=None) as postgres:
        yield postgres


@pytest.fixture
def manager(
    postgres_container: PostgresContainer,
) -> Generator[PostgreSQLStateStoreManager, None, None]:
    """Create a PostgreSQLStateStoreManager connected to the test container."""
    with PostgreSQLStateStoreManager(
        uri=postgres_container.get_connection_url(),
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
    ) as obj:
        yield obj


@pytest.mark.skipif(
    os.environ.get("CI") == "true" and platform.system() != "Linux",
    reason="Integration tests are only supported on Linux",
)
class TestIntegration:
    """Integration tests using a real PostgreSQL container."""

    def test_set_and_get_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test setting and getting state with a real database."""
        state = MeltanoState(
            state_id="integration_test_job",
            partial_state={"singer_state": {"position": 100}},
            completed_state={"singer_state": {"position": 50}},
        )

        manager.set(state)
        retrieved = manager.get("integration_test_job")

        assert retrieved is not None
        assert retrieved.state_id == "integration_test_job"
        assert retrieved.partial_state == {"singer_state": {"position": 100}}
        assert retrieved.completed_state == {"singer_state": {"position": 50}}

        # Cleanup
        manager.delete("integration_test_job")

    def test_update_existing_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test updating existing state."""
        state_id = "update_test_job"

        # Set initial state
        initial = MeltanoState(
            state_id=state_id,
            partial_state={"position": 1},
            completed_state={},
        )
        manager.set(initial)

        # Update state
        updated = MeltanoState(
            state_id=state_id,
            partial_state={"position": 2},
            completed_state={"position": 1},
        )
        manager.set(updated)

        # Verify update
        retrieved = manager.get(state_id)
        assert retrieved is not None
        assert retrieved.partial_state == {"position": 2}
        assert retrieved.completed_state == {"position": 1}

        # Cleanup
        manager.delete(state_id)

    def test_get_nonexistent_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test getting state that doesn't exist."""
        result = manager.get("nonexistent_state_id")
        assert result is None

    def test_delete_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test deleting state."""
        state_id = "delete_test_job"

        # Create state
        state = MeltanoState(state_id=state_id, partial_state={"data": 1}, completed_state={})
        manager.set(state)

        # Verify it exists
        assert manager.get(state_id) is not None

        # Delete it
        manager.delete(state_id)

        # Verify it's gone
        assert manager.get(state_id) is None

    def test_get_state_ids(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test listing all state IDs."""
        state_ids = ["list_test_1", "list_test_2", "list_test_3"]

        # Create states
        for state_id in state_ids:
            state = MeltanoState(state_id=state_id, partial_state={}, completed_state={})
            manager.set(state)

        # Get all state IDs
        retrieved_ids = list(manager.get_state_ids())

        # Verify all our states are present
        for state_id in state_ids:
            assert state_id in retrieved_ids

        # Cleanup
        for state_id in state_ids:
            manager.delete(state_id)

    def test_get_state_ids_with_pattern(
        self,
        manager: PostgreSQLStateStoreManager,
    ) -> None:
        """Test listing state IDs with a pattern filter."""
        # Create states with different prefixes
        manager.set(
            MeltanoState(state_id="pattern_alpha_1", partial_state={}, completed_state={}),
        )
        manager.set(
            MeltanoState(state_id="pattern_alpha_2", partial_state={}, completed_state={}),
        )
        manager.set(
            MeltanoState(state_id="pattern_beta_1", partial_state={}, completed_state={}),
        )

        # Filter by pattern
        alpha_ids = list(manager.get_state_ids("pattern_alpha_*"))

        assert len(alpha_ids) == 2
        assert "pattern_alpha_1" in alpha_ids
        assert "pattern_alpha_2" in alpha_ids
        assert "pattern_beta_1" not in alpha_ids

        # Cleanup
        manager.delete("pattern_alpha_1")
        manager.delete("pattern_alpha_2")
        manager.delete("pattern_beta_1")

    def test_clear_all(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test clearing all states."""
        # Create some states
        for i in range(3):
            state = MeltanoState(
                state_id=f"clear_test_{i}",
                partial_state={},
                completed_state={},
            )
            manager.set(state)

        # Clear all
        count = manager.clear_all()

        # Verify count is at least 3 (could be more from other tests)
        assert count >= 3

        # Verify states are gone
        for i in range(3):
            assert manager.get(f"clear_test_{i}") is None

    def test_acquire_lock(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test lock acquisition and release."""
        state_id = "lock_test_job"

        with manager.acquire_lock(state_id):
            # Lock should be held here
            pass  # pragma: no cover - context manager test

        # Lock should be released, we can acquire it again
        with manager.acquire_lock(state_id):
            pass  # pragma: no cover - context manager test

    def test_null_state_values(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test handling of null/None state values."""
        state_id = "null_test_job"

        # Set state with None values (empty dicts)
        state = MeltanoState(state_id=state_id, partial_state=None, completed_state=None)
        manager.set(state)

        # Retrieve and verify
        retrieved = manager.get(state_id)
        assert retrieved is not None
        assert retrieved.partial_state == {}
        assert retrieved.completed_state == {}

        # Cleanup
        manager.delete(state_id)


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve())  # type: ignore[no-any-return]


def test_get_manager(project: Project) -> None:
    with mock.patch(
        "meltano_state_backend_postgresql.backend.PostgreSQLStateStoreManager._ensure_tables",
    ) as mock_ensure_tables:
        manager = state_store_manager_from_project_settings(project.settings)

    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, PostgreSQLStateStoreManager)
    assert manager.uri == "postgresql://user:password@localhost:5432/meltano/public"
    assert manager.host == "localhost"
    assert manager.port == 5432
    assert manager.user == "user"
    assert manager.password == "password"  # noqa: S105
    assert manager.database == "meltano"
    assert manager.schema == "public"
    assert manager.sslmode == "prefer"
    assert manager.table_name == "state"


@pytest.mark.parametrize(
    ("setting_name", "env_var_name"),
    (
        pytest.param(
            "state_backend.postgresql.host",
            "MELTANO_STATE_BACKEND_POSTGRESQL_HOST",
            id="host",
        ),
        pytest.param(
            "state_backend.postgresql.port",
            "MELTANO_STATE_BACKEND_POSTGRESQL_PORT",
            id="port",
        ),
        pytest.param(
            "state_backend.postgresql.database",
            "MELTANO_STATE_BACKEND_POSTGRESQL_DATABASE",
            id="database",
        ),
        pytest.param(
            "state_backend.postgresql.schema",
            "MELTANO_STATE_BACKEND_POSTGRESQL_SCHEMA",
            id="schema",
        ),
        pytest.param(
            "state_backend.postgresql.sslmode",
            "MELTANO_STATE_BACKEND_POSTGRESQL_SSLMODE",
            id="sslmode",
        ),
        pytest.param(
            "state_backend.postgresql.table",
            "MELTANO_STATE_BACKEND_POSTGRESQL_TABLE",
            id="table",
        ),
    ),
)
def test_settings(project: Project, setting_name: str, env_var_name: str) -> None:
    setting = project.settings.find_setting(setting_name)
    assert setting is not None

    env_vars = setting.env_vars(prefixes=["meltano"])
    assert env_vars[0].key == env_var_name


@pytest.fixture
def mock_connection() -> Generator[tuple[mock.Mock, mock.Mock], None, None]:
    """Mock PostgreSQL connection."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()

        # Mock the context manager for cursor
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context

        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def subject(
    mock_connection: tuple[mock.Mock, mock.Mock],
) -> tuple[PostgreSQLStateStoreManager, mock.Mock]:
    """Create PostgreSQLStateStoreManager instance with mocked connection."""
    mock_conn, mock_cursor = mock_connection
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost:5432/testdb/testschema",
        host="testhost",
        port=5432,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
        schema="testschema",
    )
    # Replace the cached connection with our mock
    manager.connection = mock_conn
    return manager, mock_cursor


def test_set_state(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test setting state."""
    manager, mock_cursor = subject

    # Test setting new state
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)

    # Verify execute was called with correct parameters
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
    """Test getting state."""
    manager, mock_cursor = subject

    # Mock cursor response - PostgreSQL TEXT columns return strings
    mock_cursor.fetchone.return_value = (
        '{"singer_state": {"partial": 1}}',
        '{"singer_state": {"complete": 1}}',
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify execute was called with correct parameter
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == ("test_job",)

    # Verify returned state
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_with_json_strings(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting state when PostgreSQL returns JSON strings."""
    manager, mock_cursor = subject

    # Mock cursor response with JSON strings
    mock_cursor.fetchone.return_value = (
        '{"singer_state": {"partial": 1}}',
        '{"singer_state": {"complete": 1}}',
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state is properly parsed
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_with_null_values(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with NULL TEXT columns."""
    manager, mock_cursor = subject

    # Mock cursor response with None values
    mock_cursor.fetchone.return_value = (
        None,
        '{"singer_state": {"complete": 1}}',
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state handles None correctly
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_not_found(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting state that doesn't exist."""
    manager, mock_cursor = subject

    # Mock cursor response
    mock_cursor.fetchone.return_value = None

    # Get state
    state = manager.get("nonexistent")

    # Verify it returns None
    assert state is None


def test_get_state_with_none_values(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with None values (NULL in PostgreSQL)."""
    manager, mock_cursor = subject

    # Mock cursor response with None values
    mock_cursor.fetchone.return_value = (None, None)

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state has empty dicts for None values
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {}


def test_delete_state(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test deleting state."""
    manager, mock_cursor = subject

    # Delete state
    manager.delete("test_job")

    # Verify execute was called with correct parameter
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == ("test_job",)


def test_get_state_ids(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting all state IDs."""
    manager, mock_cursor = subject

    # Mock cursor response - need to make cursor itself iterable
    mock_cursor.fetchall.return_value = ["job1", "job2", "job3"]

    # Get state IDs
    state_ids = list(manager.get_state_ids())

    # Verify execute was called
    mock_cursor.execute.assert_called()

    # Verify returned IDs
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test getting state IDs with pattern."""
    manager, mock_cursor = subject

    # Mock cursor response - need to make cursor itself iterable
    mock_cursor.fetchall.return_value = ["test_job_1", "test_job_2"]

    # Get state IDs with pattern
    state_ids = list(manager.get_state_ids("test_*"))

    # Verify execute was called with correct pattern parameter
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == ("test_%",)

    # Verify returned IDs
    assert state_ids == ["test_job_1", "test_job_2"]


def test_clear_all(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test clearing all states."""
    manager, mock_cursor = subject

    # Mock count query response
    mock_cursor.fetchone.return_value = (5,)

    # Clear all
    count = manager.clear_all()

    # Verify execute was called (count + truncate)
    assert mock_cursor.execute.call_count >= 2

    # Verify returned count
    assert count == 5


def test_acquire_lock(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test acquiring and releasing lock."""
    manager, mock_cursor = subject

    # Test successful lock acquisition
    with manager.acquire_lock("test_job", retry_seconds=0):
        # Lock should be acquired
        mock_cursor.execute.assert_called()

    # After context exit, lock should be released (execute called multiple times)
    assert mock_cursor.execute.call_count >= 3  # table setup + lock + release


def test_acquire_lock_retry(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test lock retry mechanism with advisory locks."""
    manager, mock_cursor = subject

    # Mock advisory lock: first attempt fails (returns False), second succeeds (returns True)
    mock_cursor.fetchone.side_effect = [False, True]

    with (
        mock.patch("meltano_state_backend_postgresql.backend.sleep"),
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        pass

    # Verify it retried (2 lock attempts + 1 unlock)
    assert mock_cursor.execute.call_count >= 2


def test_missing_user_validation() -> None:
    """Test missing user validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="PostgreSQL user is required",
    ):
        PostgreSQLStateStoreManager(
            uri="postgresql://localhost/db",  # No user in URI
            password="pass",  # noqa: S106
            database="db",
        )


def test_missing_password_validation() -> None:
    """Test missing password validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="PostgreSQL password is required",
    ):
        PostgreSQLStateStoreManager(
            uri="postgresql://user@localhost/db",  # No password in URI
            user="test",
            database="db",
        )


def test_missing_database_validation() -> None:
    """Test missing database validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="PostgreSQL database is required",
    ):
        PostgreSQLStateStoreManager(
            uri="postgresql://user:pass@localhost/",  # No database in path
            user="test",
            password="pass",  # noqa: S106
            # No database parameter
        )


def test_connection_defaults() -> None:
    """Test connection creation with default values."""
    # Mock psycopg.connect directly during manager creation
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb",
            host="testhost",
            user="testuser",
            password="testpass",  # noqa: S106
            database="testdb",
        )

        # Access the connection property to trigger the connection
        _ = manager.connection

        # Verify defaults were used
        mock_connect.assert_called_with(
            host="testhost",
            port=5432,  # Default port
            dbname="testdb",
            user="testuser",
            password="testpass",  # noqa: S106
            sslmode="prefer",  # Default sslmode
            autocommit=True,
        )


def test_acquire_lock_max_retries_exceeded(
    subject: tuple[PostgreSQLStateStoreManager, mock.Mock],
) -> None:
    """Test lock acquisition with max retries exceeded using advisory locks."""
    manager, mock_cursor = subject

    # Mock advisory lock always returning False (lock not acquired)
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
    """Test lock acquisition with multiple retries before success using advisory locks."""
    manager, mock_cursor = subject

    # Mock advisory lock: fails 3 times, then succeeds
    mock_cursor.fetchone.side_effect = [False, False, False, True]

    with (
        mock.patch("meltano_state_backend_postgresql.backend.sleep") as mock_sleep,
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        # Verify sleep was called 3 times (for the 3 failed attempts)
        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(0.01)


def test_uri_port_parsing() -> None:
    """Test URI port parsing."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost:9999/testdb",
            user="testuser",
            password="testpass",  # noqa: S106
        )

        # Verify port was parsed from URI
        assert manager.port == 9999


def test_uri_schema_parsing() -> None:
    """Test URI schema parsing from path."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb/myschema",
            user="testuser",
            password="testpass",  # noqa: S106
        )

        # Verify schema was parsed from URI path
        assert manager.schema == "myschema"


def test_context_manager_closes_connection() -> None:
    """Test that using the manager as a context manager closes the connection."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        with PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb",
            user="testuser",
            password="testpass",  # noqa: S106
            database="testdb",
        ) as manager:
            # Connection should be open
            _ = manager.connection

        # Connection should be closed after exiting context
        mock_conn.close.assert_called_once()


def test_close_without_connection() -> None:
    """Test that close() is safe to call when no connection was opened."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb",
            user="testuser",
            password="testpass",  # noqa: S106
            database="testdb",
        )

        # Replace the cached connection with our mock so _ensure_tables works,
        # then remove it to simulate no connection being opened
        del manager.__dict__["connection"]

        # close() should not raise
        manager.close()
        mock_conn.close.assert_not_called()


def test_custom_table_name() -> None:
    """Test configurable table name."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb",
            user="testuser",
            password="testpass",  # noqa: S106
            database="testdb",
            table="custom_state_table",
        )

        assert manager.table_name == "custom_state_table"


def test_default_table_name() -> None:
    """Test default table name is 'state'."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb",
            user="testuser",
            password="testpass",  # noqa: S106
            database="testdb",
        )

        assert manager.table_name == "state"
