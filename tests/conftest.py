from __future__ import annotations

import shutil
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.project import Project
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
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve(), activate=False)  # type: ignore[no-any-return]


@pytest.fixture
def mock_connection() -> Generator[tuple[mock.MagicMock, mock.Mock, mock.Mock], None, None]:
    """Patch psycopg.connect and yield (mock_connect, mock_conn, mock_cursor)."""
    with mock.patch("psycopg.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()

        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context

        mock_connect.return_value = mock_conn
        yield mock_connect, mock_conn, mock_cursor


@pytest.fixture
def subject(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> tuple[PostgreSQLStateStoreManager, mock.Mock]:
    """PostgreSQLStateStoreManager with a mocked connection."""
    _, mock_conn, mock_cursor = mock_connection
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost:5432/testdb",
        host="testhost",
        port=5432,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
        schema="testschema",
    )
    manager.connection = mock_conn
    return manager, mock_cursor
