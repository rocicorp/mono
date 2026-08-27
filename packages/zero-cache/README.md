# zero-cache

## Testing

These require Docker, and are run with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
pnpm run test
```

### Coverage

To view test coverage in the VSCode editor:

- Install the [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) extension
- Enable Coverage Gutters Watch: `Command-Shift-8`
- Run `pnpm run test` to update coverage.

## Benchmarks

See [apps/zero-throughput](../../apps/zero-throughput/README.md) for the end-to-end throughput, capacity, fanout, and multi-process replication benchmarking harness.
