"""StateStoreManager for PostgreSQL state backend."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import psycopg
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)
from psycopg.rows import scalar_row, tuple_row
from psycopg.sql import SQL, Identifier

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from psycopg.rows import TupleRow


class PostgresStateBackendError(MeltanoError):
    """Base error for PostgreSQL state backend."""


POSTGRESQL_HOST = SettingDefinition(
    name="state_backend.postgresql.host",
    label="PostgreSQL Host",
    description="PostgreSQL server hostname",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

POSTGRESQL_PORT = SettingDefinition(
    name="state_backend.postgresql.port",
    label="PostgreSQL Port",
    description="PostgreSQL server port",
    kind=SettingKind.INTEGER,  # ty: ignore[invalid-argument-type]
    value=5432,
    env_specific=True,
)

POSTGRESQL_DATABASE = SettingDefinition(
    name="state_backend.postgresql.database",
    label="PostgreSQL Database",
    description="PostgreSQL database name",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

POSTGRESQL_USER = SettingDefinition(
    name="state_backend.postgresql.user",
    label="PostgreSQL User",
    description="PostgreSQL username",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

POSTGRESQL_PASSWORD = SettingDefinition(
    name="state_backend.postgresql.password",
    label="PostgreSQL Password",
    description="PostgreSQL password",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    sensitive=True,
    env_specific=True,
)

POSTGRESQL_SCHEMA = SettingDefinition(
    name="state_backend.postgresql.schema",
    label="PostgreSQL Schema",
    description="PostgreSQL schema name",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    value="public",
    env_specific=True,
)

POSTGRESQL_SSLMODE = SettingDefinition(
    name="state_backend.postgresql.sslmode",
    label="PostgreSQL SSL Mode",
    description="PostgreSQL SSL mode",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    value="prefer",
    env_specific=True,
)

POSTGRESQL_TABLE = SettingDefinition(
    name="state_backend.postgresql.table",
    label="PostgreSQL Table",
    description="PostgreSQL table name for state storage",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    value="state",
    env_specific=True,
)


class PostgreSQLStateStoreManager(StateStoreManager):
    """State backend for PostgreSQL."""

    label: str = "PostgreSQL"

    def __enter__(self) -> PostgreSQLStateStoreManager:
        """Enter the context manager.

        Returns:
            The manager instance.

        """
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the context manager, closing the connection."""
        self.close()

    def close(self) -> None:
        """Close the PostgreSQL connection if it has been opened."""
        if "connection" in self.__dict__:
            self.connection.close()

    def __init__(
        self,
        uri: str,
        *,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        sslmode: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the PostgresStateStoreManager.

        Args:
            uri: The state backend URI
            host: PostgreSQL server hostname
            port: PostgreSQL server port
            database: PostgreSQL database name
            user: PostgreSQL username
            password: PostgreSQL password
            schema: PostgreSQL schema name (default: public)
            sslmode: PostgreSQL SSL mode (default: prefer)
            table: PostgreSQL table name for state storage (default: state)
            kwargs: Additional keyword args to pass to parent

        """
        super().__init__(**kwargs)
        self.uri = uri
        parsed = urlparse(uri)

        # Extract connection details from URI and parameters
        self.host = host or parsed.hostname or "localhost"

        # Handle port from URI or parameter
        if port is not None:
            self.port = port
        elif parsed.port is not None:
            self.port = parsed.port
        else:
            self.port = 5432

        self.user = user or parsed.username
        if not self.user:
            msg = "PostgreSQL user is required"
            raise MissingStateBackendSettingsError(msg)

        self.password = password or parsed.password
        if not self.password:
            msg = "PostgreSQL password is required"
            raise MissingStateBackendSettingsError(msg)

        # Extract database from path
        path_parts = parsed.path.strip("/").split("/") if parsed.path else []
        self.database = database or (path_parts[0] if path_parts else None)
        if not self.database:
            msg = "PostgreSQL database is required"
            raise MissingStateBackendSettingsError(msg)

        self.schema = schema or (path_parts[1] if len(path_parts) > 1 else "public")
        self.sslmode = sslmode or "prefer"
        self.table_name = table or "state"

        self._ensure_tables()

    @cached_property
    def connection(self) -> psycopg.Connection[TupleRow]:
        """Get a PostgreSQL connection.

        Returns:
            A PostgreSQL connection object.

        """
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
            autocommit=True,
        )

    def _ensure_tables(self) -> None:
        """Ensure the state table exists."""
        with self.connection.cursor() as cursor:
            query = SQL(
                """\
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    state_id VARCHAR(900) PRIMARY KEY,
                    partial_state TEXT,
                    completed_state TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query)

    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.

        """
        partial_json = json.dumps(state.partial_state) if state.partial_state else None
        completed_json = json.dumps(state.completed_state) if state.completed_state else None

        with self.connection.cursor() as cursor:
            query = SQL(
                """\
                INSERT INTO {schema}.{table_name}
                (state_id, partial_state, completed_state, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (state_id)
                DO UPDATE SET
                    partial_state = EXCLUDED.partial_state,
                    completed_state = EXCLUDED.completed_state,
                    updated_at = CURRENT_TIMESTAMP
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query, (state.state_id, partial_json, completed_json))

    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job

        """
        with self.connection.cursor(row_factory=tuple_row) as cursor:
            query = SQL(
                """\
                SELECT partial_state, completed_state
                FROM {schema}.{table_name}
                WHERE state_id = %s
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query, (state_id,))
            row = cursor.fetchone()

            if not row:
                return None

            partial_state, completed_state = row

            # PostgreSQL returns None for NULL TEXT columns
            # but MeltanoState expects empty dicts
            # TEXT columns store JSON strings that need parsing
            return MeltanoState(
                state_id=state_id,
                partial_state=json.loads(partial_state) if partial_state is not None else {},
                completed_state=json.loads(completed_state) if completed_state is not None else {},
            )

    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for

        """
        with self.connection.cursor() as cursor:
            query = SQL(
                """\
                DELETE FROM {schema}.{table_name} WHERE state_id = %s
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query, (state_id,))

    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.

        """
        with self.connection.cursor() as cursor:
            query = SQL(
                """\
                SELECT COUNT(*) FROM {schema}.{table_name}
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query)
            count = cursor.fetchone()[0]  # type: ignore[index]
            query = SQL(
                """\
                TRUNCATE TABLE {schema}.{table_name}
                """
            ).format(
                schema=Identifier(self.schema),
                table_name=Identifier(self.table_name),
            )
            cursor.execute(query)
            return count  # type: ignore[no-any-return]

    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            An iterable of state_ids

        """
        with self.connection.cursor(row_factory=scalar_row) as cursor:
            if pattern and pattern != "*":
                # Convert glob pattern to SQL LIKE pattern
                sql_pattern = pattern.replace("*", "%").replace("?", "_")
                query = SQL(
                    """\
                    SELECT state_id FROM {schema}.{table_name}
                    WHERE state_id LIKE %s
                    """
                ).format(
                    schema=Identifier(self.schema),
                    table_name=Identifier(self.table_name),
                )
                cursor.execute(query, (sql_pattern,))
            else:
                query = SQL(
                    """\
                    SELECT state_id FROM {schema}.{table_name}
                    """
                ).format(
                    schema=Identifier(self.schema),
                    table_name=Identifier(self.table_name),
                )
                cursor.execute(query)

            yield from cursor.fetchall()

    @staticmethod
    def _state_id_to_lock_key(state_id: str) -> int:
        """Convert a state_id to a PostgreSQL advisory lock key.

        Uses MD5 hash to get a deterministic 64-bit integer from the state_id.

        Args:
            state_id: the state_id to convert

        Returns:
            A 64-bit signed integer suitable for pg_advisory_lock

        """
        # Use MD5 for deterministic hashing, take first 8 bytes as signed int64
        hash_bytes = hashlib.md5(state_id.encode()).digest()[:8]  # noqa: S324
        return int.from_bytes(hash_bytes, byteorder="big", signed=True)

    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: int = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state using PostgreSQL advisory locks.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired

        """
        lock_key = self._state_id_to_lock_key(state_id)
        max_seconds = 30
        seconds_waited = 0

        while seconds_waited < max_seconds:  # pragma: no branch
            with self.connection.cursor(row_factory=scalar_row) as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
                if cursor.fetchone():  # lock acquired
                    break
            seconds_waited += retry_seconds
            if seconds_waited >= max_seconds:
                msg = f"Could not acquire lock for state_id: {state_id}"
                raise StateIDLockedError(msg)
            sleep(retry_seconds)

        try:
            yield
        finally:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
