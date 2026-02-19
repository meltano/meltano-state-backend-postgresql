from __future__ import annotations

import os
import platform
import uuid
from typing import TYPE_CHECKING

import psycopg
import pytest
import sqlalchemy
import sqlalchemy.orm
from meltano.core.migration_service import MigrationService
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from psycopg.sql import SQL, Identifier

from meltano_state_backend_postgresql.backend import PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator

    from meltano.core.project import Project
    from testcontainers.postgres import PostgresContainer


@pytest.mark.skipif(
    os.environ.get("CI") == "true" and platform.system() != "Linux",
    reason="Integration tests are only supported on Linux",
)
class TestBackwardsCompat:
    """Test backwards compatibility with the systemdb schema.

    1. Copies the fixture project to a temporary directory
    2. Runs systemdb migrations to create the Meltano-managed tables
    3. Sets the state using the systemdb state backend and validates the state is set
    4. Gets the state using this new state backend and validates the state is the same as the one set
    """

    @pytest.fixture
    def schema(self, postgres_container: PostgresContainer) -> Generator[str, None, None]:
        """Get a unique schema for the test."""
        schema_name = f"test_schema_{uuid.uuid4().hex[:8]}"
        identifier = Identifier(schema_name)
        with psycopg.connect(postgres_container.get_connection_url(), autocommit=True) as conn:
            conn.execute(SQL("CREATE SCHEMA {schema_name}").format(schema_name=identifier))
            yield schema_name
            conn.execute(SQL("DROP SCHEMA {schema_name} CASCADE").format(schema_name=identifier))

    @pytest.fixture
    def systemdb_uri(self, postgres_container: PostgresContainer, schema: str) -> str:
        """Get the connection URL for the PostgreSQL container."""
        return f"{postgres_container.get_connection_url(driver='psycopg')}?options=-csearch_path%3D{schema}"

    @pytest.fixture
    def libpq_uri(self, postgres_container: PostgresContainer, schema: str) -> str:
        """Get the connection URL for the PostgreSQL container."""
        return f"{postgres_container.get_connection_url()}?options=-csearch_path%3D{schema}"

    @pytest.fixture
    def engine(self, systemdb_uri: str) -> Generator[sqlalchemy.Engine, None, None]:
        """Create a SQLAlchemy engine for the PostgreSQL container."""
        engine = sqlalchemy.create_engine(systemdb_uri)
        yield engine
        engine.dispose()

    def test_systemdb_backwards_compatibility(
        self,
        project: Project,
        engine: sqlalchemy.Engine,
        systemdb_uri: str,
        libpq_uri: str,
    ) -> None:
        """Test backwards compatibility with the systemdb schema."""
        state_id = "systemdb_test_job"
        state = MeltanoState(state_id=state_id, partial_state={"data": 1}, completed_state={})

        # Run migrations to create the Meltano-managed tables
        migration_service = MigrationService(engine=engine)
        migration_service.upgrade(silent=True)

        # Use the systemdb state backend
        project.settings.set("database_uri", systemdb_uri)
        project.settings.unset("state_backend.uri")
        with sqlalchemy.orm.Session(engine) as session:
            systemdb_manager = state_store_manager_from_project_settings(project.settings, session=session)
            systemdb_manager.set(state)
            systemdb_state = systemdb_manager.get(state_id)

        assert systemdb_state is not None
        assert systemdb_state.state_id == state_id
        assert systemdb_state.partial_state == {"data": 1}
        assert systemdb_state.completed_state == {}

        # Use the new state backend
        project.settings.unset("database_uri")
        project.settings.set("state_backend.uri", libpq_uri)

        # N.B. Use of the state backend as a context manager requires Meltano 4.2+

        # The 'public' should not have state
        project.settings.set("state_backend.postgresql.schema", "public")
        with state_store_manager_from_project_settings(project.settings) as new_manager:
            assert isinstance(new_manager, PostgreSQLStateStoreManager)
            new_state = new_manager.get(state_id)

        assert new_state is None

        # The schema in search_path should have state
        project.settings.unset("state_backend.postgresql.schema")
        with state_store_manager_from_project_settings(project.settings) as new_manager:
            assert isinstance(new_manager, PostgreSQLStateStoreManager)
            new_state = new_manager.get(state_id)

        assert new_state is not None
        assert new_state.state_id == state_id
        assert new_state.partial_state == {"data": 1}
        assert new_state.completed_state == {}
