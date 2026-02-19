from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from meltano.core.state_store.base import MissingStateBackendSettingsError
from psycopg.conninfo import conninfo_to_dict

from meltano_state_backend_postgresql.backend import DEFAULT_TABLE_NAME, PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from unittest import mock


# ---------------------------------------------------------------------------
# URI / conninfo parsing
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_connection")
def test_uri_port_parsing() -> None:
    """Port is parsed from the URI when not supplied as an explicit kwarg."""
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost:9999/testdb",
        user="testuser",
        password="testpass",  # noqa: S106
    )
    assert conninfo_to_dict(manager.conninfo)["port"] == "9999"


@pytest.mark.usefixtures("mock_connection")
def test_uri_sslmode_from_query_param() -> None:
    """Sslmode is read from the URI query string."""
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb?sslmode=require",
    )
    assert conninfo_to_dict(manager.conninfo)["sslmode"] == "require"


@pytest.mark.usefixtures("mock_connection")
def test_uri_schema_from_options_query_param(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Schema in ?options=-csearch_path= stays in conninfo; table identifier is unqualified."""
    with caplog.at_level(logging.INFO):
        manager = PostgreSQLStateStoreManager(
            uri="postgresql://testuser:testpass@testhost/testdb?options=-csearch_path%3Dmyschema",
        )

    assert caplog.messages == [
        f"No explicit table name provided, using default table name: {DEFAULT_TABLE_NAME}",
        "No explicit schema provided, connection will use default search path",
    ]
    assert manager.table_identifier.as_string() == f'"{DEFAULT_TABLE_NAME}"'
    assert conninfo_to_dict(manager.conninfo).get("options") == "-csearch_path=myschema"


def test_options_forwarded_to_connect(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """Options from the URI are forwarded verbatim to psycopg.connect."""
    mock_connect, _, _ = mock_connection
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb?options=-csearch_path%3Dmyschema",
    )
    _ = manager.connection
    mock_connect.assert_called_with(manager.conninfo, autocommit=True)


@pytest.mark.usefixtures("mock_connection")
def test_explicit_schema_kwarg_overrides_uri() -> None:
    """An explicit schema= kwarg takes priority over ?options= in the URI."""
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb?options=-csearch_path%3Durischema",
        user="testuser",
        password="testpass",  # noqa: S106
        schema="kwargschema",
    )
    assert manager.table_identifier.as_string() == '"kwargschema"."state"'


# ---------------------------------------------------------------------------
# Table identifier defaults
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_connection")
def test_default_table_name() -> None:
    """Default table name is used when none is provided."""
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb",
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    )
    assert manager.table_identifier.as_string() == f'"{DEFAULT_TABLE_NAME}"'


@pytest.mark.usefixtures("mock_connection")
def test_custom_table_name() -> None:
    """Custom table name is used when provided."""
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb",
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
        table="custom_state_table",
    )
    assert manager.table_identifier.as_string() == '"custom_state_table"'


# ---------------------------------------------------------------------------
# Connection defaults
# ---------------------------------------------------------------------------


def test_connection_defaults(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """psycopg.connect is called with the normalized conninfo string."""
    mock_connect, _, _ = mock_connection
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb",
        host="testhost",
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    )
    _ = manager.connection
    mock_connect.assert_called_with(manager.conninfo, autocommit=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_missing_user_validation() -> None:
    """Raises when user is absent from both the URI and kwargs."""
    with pytest.raises(MissingStateBackendSettingsError, match="PostgreSQL user is required"):
        PostgreSQLStateStoreManager(
            uri="postgresql://localhost/db",
            password="pass",  # noqa: S106
            database="db",
        )


def test_missing_password_validation() -> None:
    """Raises when password is absent from both the URI and kwargs."""
    with pytest.raises(MissingStateBackendSettingsError, match="PostgreSQL password is required"):
        PostgreSQLStateStoreManager(
            uri="postgresql://user@localhost/db",
            user="test",
            database="db",
        )


def test_missing_database_validation() -> None:
    """Raises when database is absent from both the URI and kwargs."""
    with pytest.raises(MissingStateBackendSettingsError, match="PostgreSQL database is required"):
        PostgreSQLStateStoreManager(
            uri="postgresql://user:pass@localhost/",
            user="test",
            password="pass",  # noqa: S106
        )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_context_manager_closes_connection(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """Exiting the context manager closes the connection."""
    _, mock_conn, _ = mock_connection
    with PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb",
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    ) as manager:
        _ = manager.connection

    mock_conn.close.assert_called_once()


def test_close_without_connection(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """close() is safe to call when the connection was never opened."""
    _, mock_conn, _ = mock_connection
    manager = PostgreSQLStateStoreManager(
        uri="postgresql://testuser:testpass@testhost/testdb",
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    )
    del manager.__dict__["connection"]
    manager.close()
    mock_conn.close.assert_not_called()
