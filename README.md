# Rocicorp Monorepo

This is the mono repo for [Rocicorp](https://rocicorp.dev/)'s two main products (dec 2024).

## Zero

Web site and end user docs: https://zero.rocicorp.dev/.

The code for this is in a few different packages in this repo:

- [packages/zero-client](./packages/zero-client): The main client library. It use replicache under the hood.
- [packages/zero-cache](./packages/zero-cache): The server side code.
- [packages/zql](./packages/zql): The IVM (incremental view maintenance) engine as well as the query language/API.
- [@rocicorp/zero-docs](https://github.com/rocicorp/zero-docs): The docs for zero is currently in a separate repo.

## Replicache

Web site and end user docs https://replicache.dev/.

### Code

- [packages/replicache](./packages/replicache): The replicache client library.
- [packages/replicache/doc](./packages/replicache/): The docs for replicache is in this mono repo.

## Older Projects

### Reflect

Reflect is no longer under development. The code lives in a the [@rocicorp/reflect-archive](https://github.com/rocicorp/reflect-archive) repo.
