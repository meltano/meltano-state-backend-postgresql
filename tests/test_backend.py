# Tests have been split into focused modules:
#   test_conninfo.py    - URI parsing, validation, and connection lifecycle
#   test_state.py       - state CRUD and locking (unit)
#   test_integration.py - integration tests against a real PostgreSQL container
#   test_meltano.py     - Meltano project/settings integration
