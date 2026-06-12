# Dev containers

The dev container is the sandbox for normal development and AI-agent work.
To keep it a meaningful sandbox, it must not be able to create or control
containers:

- No Docker daemon inside the workspace (no Docker-in-Docker, which requires
  privileged mode and materially weakens the container boundary).
- No host Docker socket mounted into the workspace.
- Docker Compose is orchestrated by host-side Dev Containers tooling, not by
  code or agents running inside the workspace.

Services the workspace needs (Postgres for tests and for zbugs) run as
**sibling containers** in the same Compose project. The workspace reaches
them over the Compose network by service name, but cannot manage them.

## Profiles

### Default — `devcontainer.json`

The workspace plus four static Postgres containers (`pg15`–`pg18`), one per
major version exercised by the pg test matrix. The `TEST_PG_<major>` env vars
point `packages/zero-cache/test/pg-container-setup.ts` at these services, so
the `*.pg.test.ts` suites run with plain `pnpm test` — no Docker daemon, no
testcontainers. Outside the dev container (host, CI) those env vars are unset
and the tests start their own containers via testcontainers, as before.

The Postgres server flags and timezones in `docker-compose.yml` must stay in
sync with `packages/zero-cache/test/pg-container-setup.ts` and
`packages/zero-cache/test/pg-1*.ts`.

### zbugs — `zbugs/devcontainer.json`

Everything in the default profile, plus the zbugs Postgres services from
`apps/zbugs/docker/docker-compose.yml` started as siblings
(`postgres_primary`, `postgres_replica`). Use this profile to run zbugs
locally (VS Code: "Reopen in Container" and pick "Mono — zbugs").

Inside this profile:

- Do **not** run `pnpm run db-up` / `db-down` — Postgres is already running.
- `ZERO_UPSTREAM_DB` is preset to
  `postgres://user:password@postgres_primary:5432/postgres`; connect by
  Compose service name, not `localhost:6434` (inside the workspace,
  `localhost` is the workspace itself).
- `pnpm run db-migrate`, `db-seed`, `zero-cache-dev`, and `dev` work as
  documented in `apps/zbugs/README.md`.

Do not run `pnpm run db-up` on the host while the zbugs profile is running;
both publish host ports 6434/6435.

## Operations that need Docker

Restarting, rebuilding, or wiping the sibling services happens from the host
(Dev Containers: "Rebuild Container", or `docker compose`/`docker volume`
commands against the Compose project). Do not give the workspace Docker
access to make this convenient. The test Postgres containers keep their data
on tmpfs, so a restart fully resets them; the zbugs databases persist in the
`zbugs_pgdata_*` named volumes.
