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

## Opening it

Use **"Dev Containers: Clone Repository in Container Volume…"** (Command
Palette) and point it at this repo / branch. The Dev Containers tooling
clones the repo into a Docker volume and mounts it at `/workspaces`, so the
only thing you need on the host is **Docker + VS Code + the Dev Containers
extension** — no host `git`, no host Node, no host checkout. This is the
supported entry point and the one the rollout standardizes on.

The `dev` service has **no host bind mounts** by design. That is what makes
Clone-in-Volume work: a `..:/workspaces/mono` host bind would have nothing to
bind to on a machine without a checkout, which is exactly the failure mode
this avoids. (Consequence: the source lives in the volume and is edited
through VS Code, not browsable on the host filesystem. On macOS this is also
substantially faster than a bind mount.) Git identity inside the container
comes from the `gh` login the `agents` feature persists, not a mounted
`~/.gitconfig`.

## Profiles

### Default — `devcontainer.json`

The workspace plus four static Postgres containers (`pg15`–`pg18`), one per
major version exercised by the pg test matrix. The `TEST_PG_<major>` env vars
point `packages/zero-cache/test/pg-container-setup.ts` at these services, so
the `*.pg.test.ts` suites run with plain `pnpm test` — no Docker daemon, no
testcontainers. `pnpm install` runs automatically on first create
(`postCreateCommand`). Outside the dev container (host, CI) those env vars
are unset and the tests start their own containers via testcontainers, as
before.

The Postgres server flags and timezones in `docker-compose.yml` must stay in
sync with `packages/zero-cache/test/pg-container-setup.ts` and
`packages/zero-cache/test/pg-1*.ts`.

### zbugs — `zbugs/devcontainer.json`

Everything in the default profile, plus the zbugs Postgres services from
`apps/zbugs/docker/docker-compose.yml` started as siblings
(`postgres_primary`, `postgres_replica`). Use this profile to run zbugs
locally: Clone in Container Volume as above, and when prompted for the
configuration pick "Mono — zbugs" (or switch with "Dev Containers: Switch
Container").

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
