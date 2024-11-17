# zero-cache

## Testing

These require Docker, and are run with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
npm run test
```

### Coverage

To view test coverage in the VSCode editor:

- Install the [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) extension
- Enable Coverage Gutters Watch: `Command-Shift-8`
- Run `npm run test` to update coverage.
