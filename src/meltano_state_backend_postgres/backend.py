"""StateStoreManager for PostgreSQL state backend."""

from __future__ import annotations

import json
import typing as t
from contextlib import contextmanager
from functools import cached_property
from time import sleep
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

if t.TYPE_CHECKING:
    from collections.abc import Generator, Iterable


class PostgresStateBackendError(MeltanoError):
    """Base error for PostgreSQL state backend."""


POSTGRES_HOST = SettingDefinition(
    name="state_backend.postgres.host",
    label="PostgreSQL Host",
    description="PostgreSQL server hostname",
    kind=SettingKind.STRING,
    env_specific=True,
)

POSTGRES_PORT = SettingDefinition(
    name="state_backend.postgres.port",
    label="PostgreSQL Port",
    description="PostgreSQL server port",
    kind=SettingKind.INTEGER,
    default=5432,
    env_specific=True,
)

POSTGRES_DATABASE = SettingDefinition(
    name="state_backend.postgres.database",
    label="PostgreSQL Database",
    description="PostgreSQL database name",
    kind=SettingKind.STRING,
    env_specific=True,
)

POSTGRES_USER = SettingDefinition(
    name="state_backend.postgres.user",
    label="PostgreSQL User",
    description="PostgreSQL username",
    kind=SettingKind.STRING,
    env_specific=True,
)

POSTGRES_PASSWORD = SettingDefinition(
    name="state_backend.postgres.password",
    label="PostgreSQL Password",
    description="PostgreSQL password",
    kind=SettingKind.STRING,
    sensitive=True,
    env_specific=True,
)

POSTGRES_SCHEMA = SettingDefinition(
    name="state_backend.postgres.schema",
    label="PostgreSQL Schema",
    description="PostgreSQL schema name",
    kind=SettingKind.STRING,
    default="public",
    env_specific=True,
)

POSTGRES_SSLMODE = SettingDefinition(
    name="state_backend.postgres.sslmode",
    label="PostgreSQL SSL Mode",
    description="PostgreSQL SSL mode",
    kind=SettingKind.STRING,
    default="prefer",
    env_specific=True,
)


class PostgresStateStoreManager(StateStoreManager):
    """State backend for PostgreSQL."""

    label: str = "PostgreSQL"
    table_name: str = "meltano_state"
    lock_table_name: str = "meltano_state_locks"

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
        **kwargs: t.Any,
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

        self._ensure_tables()

    @cached_property
    def connection(self) -> psycopg.Connection[t.Any]:
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
        """Ensure the state and lock tables exist."""
        with self.connection.cursor() as cursor:
            # Create state table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
                    state_id VARCHAR(900) PRIMARY KEY,
                    partial_state TEXT,
                    completed_state TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )

            # Create lock table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.lock_table_name} (
                    state_id VARCHAR(900) PRIMARY KEY,
                    locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    lock_id VARCHAR(255)
                )
                """,
            )

    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.

        """
        partial_json = json.dumps(state.partial_state) if state.partial_state else None
        completed_json = json.dumps(state.completed_state) if state.completed_state else None

        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {self.schema}.{self.table_name}
                    (state_id, partial_state, completed_state)
                VALUES (%s, %s, %s)
                ON CONFLICT (state_id)
                DO UPDATE SET
                    partial_state = EXCLUDED.partial_state,
                    completed_state = EXCLUDED.completed_state,
                    updated_at = CURRENT_TIMESTAMP
                """,  # noqa: S608
                (state.state_id, partial_json, completed_json),
            )

    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT partial_state, completed_state
                FROM {self.schema}.{self.table_name}
                WHERE state_id = %s
                """,  # noqa: S608
                (state_id,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            # PostgreSQL returns None for NULL TEXT columns
            # but MeltanoState expects empty dicts
            # TEXT columns store JSON strings that need parsing
            partial_state = row[0]
            completed_state = row[1]

            # Handle None values
            if partial_state is None:
                partial_state = {}
            # Parse JSON string
            elif isinstance(partial_state, str):
                partial_state = json.loads(partial_state)

            if completed_state is None:
                completed_state = {}
            # Parse JSON string
            elif isinstance(completed_state, str):
                completed_state = json.loads(completed_state)

            return MeltanoState(
                state_id=state_id,
                partial_state=partial_state,
                completed_state=completed_state,
            )

    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self.schema}.{self.table_name} WHERE state_id = %s",  # noqa: S608
                (state_id,),
            )

    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}",  # noqa: S608
            )
            count = cursor.fetchone()[0]  # type: ignore[index]
            cursor.execute(
                f"TRUNCATE TABLE {self.schema}.{self.table_name}",
            )
            return count  # type: ignore[no-any-return]

    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            An iterable of state_ids

        """
        with self.connection.cursor() as cursor:
            if pattern and pattern != "*":
                # Convert glob pattern to SQL LIKE pattern
                sql_pattern = pattern.replace("*", "%").replace("?", "_")
                cursor.execute(
                    f"""
                    SELECT state_id FROM {self.schema}.{self.table_name}
                    WHERE state_id LIKE %s
                    """,  # noqa: S608
                    (sql_pattern,),
                )
            else:
                cursor.execute(
                    f"SELECT state_id FROM {self.schema}.{self.table_name}",  # noqa: S608
                )

            for row in cursor:
                yield row[0]

    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: int = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired

        """
        import uuid

        lock_id = str(uuid.uuid4())
        max_seconds = 30
        seconds_waited = 0

        while seconds_waited < max_seconds:  # pragma: no branch
            try:
                with self.connection.cursor() as cursor:
                    # Try to acquire lock
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.{self.lock_table_name}
                            (state_id, lock_id)
                        VALUES (%s, %s)
                        """,  # noqa: S608
                        (state_id, lock_id),
                    )
                    break
            except psycopg.errors.UniqueViolation as e:
                seconds_waited += retry_seconds
                if seconds_waited >= max_seconds:  # Last attempt
                    msg = f"Could not acquire lock for state_id: {state_id}"
                    raise StateIDLockedError(msg) from e
                sleep(retry_seconds)

        try:
            yield
        finally:
            # Release the lock
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.{self.lock_table_name}
                    WHERE state_id = %s AND lock_id = %s
                    """,  # noqa: S608
                    (state_id, lock_id),
                )

            # Clean up old locks (older than 5 minutes)
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.{self.lock_table_name}
                    WHERE locked_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                    """,  # noqa: S608
                )
