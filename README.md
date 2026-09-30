# Rocicorp Monorepo

This is the mono repo for [Rocicorp](https://rocicorp.dev/)'s two main products, Zero and Replicache.

## About this repository

rocicorp/mono is published from Rocicorp's internal repository. Each commit
here comes from an internal commit, which the `Mono-Internal-Commit:` line at
the end of the commit message records; internal commits that only touch
private files aren't published. Releases of Zero are built and
published from this repository.

Test suites are included for [zero-client](./packages/zero-client),
[replicache](./packages/replicache), [zero-events](./packages/zero-events),
[zero-react](./packages/zero-react), [zero-solid](./packages/zero-solid),
[shared](./packages/shared) and [go](./go). The test harnesses (helpers, fixtures, vitest
configs and the Postgres test setup) are included for every package.

## Contributing

Pull requests are welcome. We don't merge them here: a maintainer ports your
change into the internal repository, and it comes back here in a commit that
lists you as a co-author and closes your pull request. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Zero

Web site and docs: https://zero.rocicorp.dev/.

### Code

- [packages/zero-client](./packages/zero-client): The main client library. It uses replicache under the hood.
- [packages/zero-cache](./packages/zero-cache): The server side code.
- [packages/zql](./packages/zql): The IVM (incremental view maintenance) engine as well as the query language/API.
- [@rocicorp/zero-docs](https://github.com/rocicorp/zero-docs): The docs for zero is currently in a separate repo.
- [apps/zbugs](./apps/zbugs): The bug tracker app.

## Replicache

Web site and docs https://replicache.dev/.

### Code

- [packages/replicache](./packages/replicache): The replicache client library.
- [packages/replicache-doc](./packages/replicache-doc): The Replicache documentation is in this monorepo.

## Older Projects

### Reflect

Reflect is no longer under development. The code lives in the [rocicorp/reflect-archive](https://github.com/rocicorp/reflect-archive) repo.

<!-- comment -->
