# verify-package-deps

A tool to verify that all internal workspace dependencies are properly declared in `package.json` files across the monorepo.

## Usage

From the workspace root:

```bash
# Check for dependency issues
npm run verify-deps

# Check and automatically fix issues
npm run verify-deps:fix
```

From this package directory:

```bash
# Check for dependency issues
npm run verify

# Check and automatically fix issues
npm run verify:fix
```

## What it does

This tool:

1. Scans all TypeScript files in `packages/`, `apps/`, and `tools/` directories
2. Extracts import statements to detect cross-package dependencies
3. Verifies that all used workspace packages are declared in `package.json`
4. Checks for version mismatches between actual and declared versions
5. Identifies unused workspace dependencies
6. Can automatically fix issues with the `--fix` flag

## Ignoring circular dependencies

If you have a legitimate circular dependency that you want to ignore, add a comment:

```typescript
// @circular-dep-ignore
import {something} from '../other-package';
```

or on the previous line:

```typescript
// @circular-dep-ignore
import {something} from '../other-package';
```
