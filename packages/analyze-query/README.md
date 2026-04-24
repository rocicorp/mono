# Analyze ZQL

This package contains:

1. `transform-query`: a script that transforms query hashes to their full
   AST and ZQL representation, with permissions applied.
2. `runAnalyzeCLI`: a library entry point for building a project-specific
   `analyze` CLI that analyzes ZQL queries against a running `zero-cache`
   via the inspector protocol. See `apps/zbugs/scripts/analyze.ts` for a
   minimal example, or import it as:

   ```ts
   import {runAnalyzeCLI} from '@rocicorp/zero/analyze';
   import {schema} from './schema.ts';
   await runAnalyzeCLI({schema});
   ```

## Usage

Run `transform-query` from the folder that contains the `.env` for your
product; it needs access to the schema, permissions, replica, and cvr db.

```bash
npx transform-query --hash=hash --schema=path_to_schema.ts
```
