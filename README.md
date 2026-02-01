# `meltano-state-backend-postgres`

[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-postgres.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-postgres)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-postgres.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-postgres)

This is a [Meltano][meltano] extension that provides a [PostgreSQL][postgresql] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with meltano-state-backend-postgres meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano meltano-state-backend-postgres
```

## Configuration

To store state in PostgreSQL, set the `state_backend.uri` setting to `postgres://<user>:<password>@<host>:<port>/<database>/<schema>`.

State will be stored in two tables that Meltano will create automatically:
- `meltano_state` - Stores the actual state data
- `meltano_state_locks` - Manages concurrency locks

To authenticate to PostgreSQL, you'll need to provide:

```yaml
state_backend:
  uri: postgres://my_user:my_password@localhost:5432/my_database/my_schema
  postgres:
    sslmode: prefer  # Optional: SSL mode (default: prefer)
```

Alternatively, you can provide credentials via individual settings:

```yaml
state_backend:
  uri: postgres://localhost/my_database
  postgres:
    host: localhost
    port: 5432         # Defaults to 5432 if not specified
    user: my_user
    password: my_password
    database: my_database
    schema: my_schema  # Defaults to public if not specified
    sslmode: prefer    # Optional: prefer, require, disable, allow, verify-ca, verify-full
```

#### Connection Parameters

- **host**: PostgreSQL server hostname (default: localhost)
- **port**: PostgreSQL server port (default: 5432)
- **user**: The username for authentication
- **password**: The password for authentication
- **database**: The database where state will be stored
- **schema**: The schema where state tables will be created (defaults to public)
- **sslmode**: SSL mode for the connection (default: prefer)

#### Security Considerations

When storing credentials:
- Use environment variables for sensitive values in production
- Consider using SSL/TLS connections with sslmode=require or verify-full
- Ensure the user has CREATE TABLE, INSERT, UPDATE, DELETE, and SELECT privileges on the schema

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_POSTGRES_PASSWORD='my_secure_password'
meltano config meltano set state_backend.uri 'postgres://my_user@localhost/my_database'
meltano config meltano set state_backend.postgres.sslmode 'require'
```

## Development

### Setup

```bash
uv sync
```

### Run tests

Run all tests, type checks, linting, and coverage:

```bash
uvx --with tox-uv tox run-parallel
```

### Bump the version

```bash
uv version --bump <type>
```

[meltano]: https://meltano.com
[postgresql]: https://www.postgresql.org/
[state-backend]: https://docs.meltano.com/concepts/state_backends
[pipx]: https://github.com/pypa/pipx
[uv]: https://docs.astral.sh/uv
