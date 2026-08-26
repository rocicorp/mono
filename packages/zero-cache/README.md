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

Zero-Cache includes an end-to-end throughput and latency benchmark harness:

```bash
# Sweep write rates to test physical ingestion limits:
pnpm run bench:capacity --write-rates 1000,2000,3000,4000

# Sweep client counts to test View-Syncer fanout scalability:
pnpm run bench:fanout --clients 5,10,20,50 --write-rate 200
```

See [bench/pipeline/README.md](file:///Users/gregorybaker/github/mono/packages/zero-cache/bench/pipeline/README.md) for full configuration options, commands, and metric guides.
