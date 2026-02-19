from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.state_store import state_store_manager_from_project_settings
from psycopg.conninfo import conninfo_to_dict

from meltano_state_backend_postgresql.backend import PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from meltano.core.project import Project


def test_get_manager(project: Project) -> None:
    with mock.patch(
        "meltano_state_backend_postgresql.backend.PostgreSQLStateStoreManager._ensure_tables",
    ) as mock_ensure_tables:
        manager = state_store_manager_from_project_settings(project.settings)

    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, PostgreSQLStateStoreManager)
    assert manager.uri == "postgresql://user:password@localhost:5432/meltano"
    assert conninfo_to_dict(manager.conninfo) == {
        "host": "localhost",
        "port": "5432",
        "user": "user",
        "password": "password",
        "dbname": "meltano",
        "sslmode": "prefer",
    }
    assert manager.table_identifier.as_string() == '"state"'


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
