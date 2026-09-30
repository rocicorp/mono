# Contributing

Thanks for helping out. This repository is published from Rocicorp's internal
repository, so pull requests work a little differently than usual.

## How your pull request lands

1. Open a pull request against `main` as usual.
2. A maintainer reviews it here.
3. Once it's ready, the maintainer ports it into the internal repository.
4. When the change is merged internally, it is published back to this
   repository in a commit that lists you with `Co-authored-by`, so GitHub
   credits you.
5. That commit says `Closes rocicorp/mono#<your PR>`, so it closes your pull
   request and links to it.

Your pull request shows as closed rather than merged. That is expected; the
commit that closed it is the merge.

## Setting up

You need a Node.js version allowed by `engines` in `package.json` (22.12+, 24,
or 26+) and the pnpm version pinned there (`packageManager`).

```bash
pnpm install
pnpm run build
```

## Running tests

Test suites are included for these packages:

- `packages/zero-client`
- `packages/replicache`
- `packages/zero-events`
- `packages/zero-react`
- `packages/zero-solid`
- `packages/shared`
- `go` (run with `make test` in `go/`)

Run a package's tests with, for example:

```bash
pnpm --filter zero-client run test
```

Some packages also run their tests in real browsers, for example
`pnpm --filter replicache run test:chromium`. Install the browsers first with
`pnpm exec playwright install`.

Other packages don't include their test suites, but their test harnesses are
here: helpers, fixtures, vitest configs and the Postgres test setup, which
starts Postgres with Docker via testcontainers. You can use them to write
tests for your change. Include those tests in your pull request: we run them
and keep them with your change internally, but they won't appear in this
repository after it's merged.

## Before you open a pull request

```bash
pnpm run check-types
pnpm run lint
pnpm run format
```

Commit messages and pull request titles use
[conventional commits](https://www.conventionalcommits.org/), for example
`fix(zero-client): handle reconnect during poke`.
