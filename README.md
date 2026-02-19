# `meltano-state-backend-postgresql`

<!-- Display these if and when we publish to PyPI. -->

<!--
[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-postgresql.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-postgresql)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-postgresql.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-postgresql) -->

This is a [Meltano] extension that provides a [PostgreSQL] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-postgresql.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-postgresql.git
```

## Configuration

To store state in PostgreSQL, set the `state_backend.uri` setting to a standard PostgreSQL connection URI:

```
postgresql://<user>:<password>@<host>:<port>/<database>?options=-csearch_path%3D<schema>
```

The `?options=-csearch_path%3D<schema>` query parameter sets the schema where state tables will be created. If omitted, the connection uses the server's default `search_path`. `%3D` is the URL-encoded form of `=`.

State will be stored in a table that Meltano will create automatically:

- `state` - Stores the actual state data

To authenticate to PostgreSQL, you'll need to provide:

```yaml
state_backend:
  uri: postgresql://my_user:my_password@localhost:5432/my_database?options=-csearch_path%3Dmy_schema
  postgresql:
    sslmode: prefer  # Optional: SSL mode
```

Alternatively, you can provide credentials via individual settings:

```yaml
state_backend:
  uri: postgresql://localhost/my_database
  postgresql:
    host: localhost
    port: 5432         # Defaults to 5432 if not specified
    user: my_user
    password: my_password
    database: my_database
    schema: my_schema  # Optional: defaults to the server's search_path if not specified
    sslmode: prefer    # Optional: prefer, require, disable, allow, verify-ca, verify-full
```

#### Connection Parameters

- **host**: PostgreSQL server hostname (default: localhost)
- **port**: PostgreSQL server port (default: 5432)
- **user**: The username for authentication
- **password**: The password for authentication
- **database**: The database where state will be stored
- **schema**: The schema where state tables will be created (defaults to the server's search_path)
- **sslmode**: SSL mode for the connection

#### Security Considerations

When storing credentials:

- Use environment variables for sensitive values in production
- Consider using SSL/TLS connections with sslmode=require or verify-full
- Ensure the user has CREATE TABLE, INSERT, UPDATE, DELETE, and SELECT privileges on the schema

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_POSTGRESQL_PASSWORD='my_secure_password'
meltano config set meltano state_backend.uri 'postgresql://my_user@localhost/my_database?options=-csearch_path%3Dmy_schema'
meltano config set meltano state_backend.postgresql.sslmode 'require'
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

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[pipx]: https://github.com/pypa/pipx
[postgresql]: https://www.postgresql.org/
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
