# zero-cache

## Testing

These require Docker, and are run with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
npm run test
```

### Coverage

To view test coverage in the VSCode editor:

- Install the [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) extension
- Update two settings to teach the extension how to find vitest coverage files:
  1. **Coverage Base Directory**: `**/coverage`
  2. **Coverage File Names**: Add `clover.xml` to the array in the JSON file
- Run `npm run test -- --coverage`
