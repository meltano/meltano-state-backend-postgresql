from __future__ import annotations

import os
import platform
from typing import TYPE_CHECKING

import psycopg
import pytest
from meltano.core.state_store import MeltanoState
from psycopg.rows import namedtuple_row
from psycopg.sql import SQL, Identifier

from meltano_state_backend_postgresql.backend import DEFAULT_TABLE_NAME, PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator

    from testcontainers.postgres import PostgresContainer


@pytest.mark.skipif(
    os.environ.get("CI") == "true" and platform.system() != "Linux",
    reason="Integration tests are only supported on Linux",
)
@pytest.mark.parametrize(
    ("schema", "table", "schema_in_options"),
    (
        pytest.param(None, None, False, id="no_schema_or_table"),
        pytest.param("test_schema", None, False, id="schema_only"),
        pytest.param(None, "test_table", False, id="table_only"),
        pytest.param("test_schema", None, True, id="schema_in_options"),
        pytest.param("test_schema", "test_table", False, id="schema_and_table"),
        pytest.param("test_schema", "test_table", True, id="schema_in_options_and_table"),
    ),
)
class TestIntegration:
    """Integration tests using a real PostgreSQL container."""

    @pytest.fixture
    def manager(
        self,
        postgres_container: PostgresContainer,
        schema: str | None,
        table: str | None,
        schema_in_options: bool,  # noqa: FBT001
    ) -> Generator[PostgreSQLStateStoreManager, None, None]:
        uri = postgres_container.get_connection_url()
        kwargs = {}
        if schema_in_options:
            uri = f"{uri}?options=-csearch_path%3D{schema}"
        else:
            kwargs["schema"] = schema

        with psycopg.connect(uri) as conn, conn.cursor() as cursor:
            if schema:
                cursor.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(schema=Identifier(schema)),
                )

        with PostgreSQLStateStoreManager(
            uri=uri,
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            table=table,
            **kwargs,
        ) as manager:
            yield manager

    def _assert_table_identifier(
        self,
        identifier: Identifier,
        schema: str | None,
        table: str | None,
        schema_in_options: bool,  # noqa: FBT001
    ) -> None:
        match schema, table, schema_in_options:
            case None, None, _:
                assert identifier.as_string() == f'"{DEFAULT_TABLE_NAME}"'
            case None, table, _:
                assert identifier.as_string() == f'"{table}"'
            case schema, None, False:
                assert identifier.as_string() == f'"{schema}"."{DEFAULT_TABLE_NAME}"'
            case schema, None, True:
                assert identifier.as_string() == f'"{DEFAULT_TABLE_NAME}"'
            case schema, table, False:
                assert identifier.as_string() == f'"{schema}"."{table}"'
            case schema, table, True:
                assert identifier.as_string() == f'"{table}"'
            case _:  # pragma: no cover
                msg = f"Unexpected schema and table combination: {schema}, {table}"
                raise AssertionError(msg)

    def test_set_and_get_state(
        self,
        manager: PostgreSQLStateStoreManager,
        schema: str | None,
        table: str | None,
        schema_in_options: bool,  # noqa: FBT001
    ) -> None:
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

        identifier = manager.table_identifier
        self._assert_table_identifier(identifier, schema, table, schema_in_options)

        explicit = Identifier(schema, table or DEFAULT_TABLE_NAME) if schema else identifier
        with psycopg.connect(manager.conninfo) as conn, conn.cursor(row_factory=namedtuple_row) as cursor:
            cursor.execute(SQL("SELECT * FROM {table_identifier}").format(table_identifier=explicit))
            rows = cursor.fetchall()
            assert len(rows) == 1

        manager.delete("integration_test_job")

    def test_update_existing_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test updating existing state."""
        state_id = "update_test_job"

        manager.set(MeltanoState(state_id=state_id, partial_state={"position": 1}, completed_state={}))
        manager.set(MeltanoState(state_id=state_id, partial_state={"position": 2}, completed_state={"position": 1}))

        retrieved = manager.get(state_id)
        assert retrieved is not None
        assert retrieved.partial_state == {"position": 2}
        assert retrieved.completed_state == {"position": 1}

        manager.delete(state_id)

    def test_get_nonexistent_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test getting state that doesn't exist."""
        assert manager.get("nonexistent_state_id") is None

    def test_delete_state(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test deleting state."""
        state_id = "delete_test_job"
        manager.set(MeltanoState(state_id=state_id, partial_state={"data": 1}, completed_state={}))
        assert manager.get(state_id) is not None
        manager.delete(state_id)
        assert manager.get(state_id) is None

    def test_get_state_ids(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test listing all state IDs."""
        state_ids = ["list_test_1", "list_test_2", "list_test_3"]
        for state_id in state_ids:
            manager.set(MeltanoState(state_id=state_id, partial_state={}, completed_state={}))

        retrieved_ids = list(manager.get_state_ids())
        for state_id in state_ids:
            assert state_id in retrieved_ids

        for state_id in state_ids:
            manager.delete(state_id)

    def test_get_state_ids_with_pattern(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test listing state IDs with a pattern filter."""
        manager.set(MeltanoState(state_id="pattern_alpha_1", partial_state={}, completed_state={}))
        manager.set(MeltanoState(state_id="pattern_alpha_2", partial_state={}, completed_state={}))
        manager.set(MeltanoState(state_id="pattern_beta_1", partial_state={}, completed_state={}))

        alpha_ids = list(manager.get_state_ids("pattern_alpha_*"))
        assert len(alpha_ids) == 2
        assert "pattern_alpha_1" in alpha_ids
        assert "pattern_alpha_2" in alpha_ids
        assert "pattern_beta_1" not in alpha_ids

        manager.delete("pattern_alpha_1")
        manager.delete("pattern_alpha_2")
        manager.delete("pattern_beta_1")

    def test_clear_all(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test clearing all states."""
        for i in range(3):
            manager.set(MeltanoState(state_id=f"clear_test_{i}", partial_state={}, completed_state={}))

        count = manager.clear_all()
        assert count >= 3
        for i in range(3):
            assert manager.get(f"clear_test_{i}") is None

    def test_acquire_lock(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test lock acquisition and release."""
        state_id = "lock_test_job"
        with manager.acquire_lock(state_id):
            pass  # pragma: no cover - context manager test
        with manager.acquire_lock(state_id):
            pass  # pragma: no cover - context manager test

    def test_null_state_values(self, manager: PostgreSQLStateStoreManager) -> None:
        """Test handling of null/None state values."""
        state_id = "null_test_job"
        manager.set(MeltanoState(state_id=state_id, partial_state=None, completed_state=None))

        retrieved = manager.get(state_id)
        assert retrieved is not None
        assert retrieved.partial_state == {}
        assert retrieved.completed_state == {}

        manager.delete(state_id)
