from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import sqlalchemy
import sqlalchemy.orm
from meltano.core.migration_service import MigrationService
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings

from meltano_state_backend_postgresql.backend import PostgreSQLStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator

    from meltano.core.project import Project
    from testcontainers.postgres import PostgresContainer


class TestBackwardCompat:
    """Test backward compatibility with the systemdb schema.

    1. Copies the fixture project to a temporary directory
    2. Runs systemdb migrations to create the Meltano-managed tables
    3. Sets the state using the systemdb state backend and validates the state is set
    4. Gets the state using this new state backend and validates the state is the same as the one set
    """

    @pytest.fixture
    def engine(self, postgres_container: PostgresContainer) -> Generator[sqlalchemy.Engine, None, None]:
        """Create a SQLAlchemy engine for the PostgreSQL container."""
        engine = sqlalchemy.create_engine(postgres_container.get_connection_url(driver="psycopg"))
        yield engine
        engine.dispose()

    def test_systemdb_backwards_compatibility(
        self,
        project: Project,
        engine: sqlalchemy.Engine,
        postgres_container: PostgresContainer,
        # session: sqlalchemy.orm.Session,
    ) -> None:
        """Test backwards compatibility with the systemdb schema."""
        state_id = "systemdb_test_job"
        state = MeltanoState(state_id=state_id, partial_state={"data": 1}, completed_state={})

        # Run migrations to create the Meltano-managed tables
        migration_service = MigrationService(engine=engine)
        migration_service.upgrade(silent=True)

        # Use the systemdb state backend
        project.settings.set("database_uri", postgres_container.get_connection_url(driver="psycopg"))
        project.settings.unset("state_backend.uri")
        with sqlalchemy.orm.Session(engine) as session:
            systemdb_manager = state_store_manager_from_project_settings(project.settings, session=session)
            systemdb_manager.set(state)
            systemdb_state = systemdb_manager.get(state_id)

        assert systemdb_state is not None
        assert systemdb_state.state_id == state_id
        assert systemdb_state.partial_state == {"data": 1}
        assert systemdb_state.completed_state == {}

        # # Use the new state backend
        project.settings.unset("database_uri")
        project.settings.set("state_backend.uri", postgres_container.get_connection_url())

        # Use as context manager requires Meltano 4.2+
        with state_store_manager_from_project_settings(project.settings) as new_manager:
            assert isinstance(new_manager, PostgreSQLStateStoreManager)
            new_manager.set(state)

            # Get the state using the new state backend
            new_state = new_manager.get(state_id)
            assert new_state is not None
            assert new_state.state_id == state_id
            assert new_state.partial_state == {"data": 1}
            assert new_state.completed_state == {}
