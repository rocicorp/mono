# zero-swift

The command-line tool for [Zero for Swift](https://github.com/wevm/zero-swift).
Your Zero app's TypeScript schema, queries, and mutators stay the source of
truth, and this tool generates what the Swift client needs from them.

```sh
npm install --save-dev zero-swift
```

It needs Node 22.18 or later, which loads TypeScript files directly. It also
needs the app's own `@rocicorp/zero`, 1.11 or later.

## `zero-swift codegen`

Generates Swift from the TypeScript schema. For each table it writes a row
struct, a `ZeroSchema`, and a `zql` builder with a `Query`.

```sh
npx zero-swift codegen            # reads zero-swift.json
npx zero-swift codegen --check    # fails when the output is out of date
```

```json
{
  "schema": "../web/shared/schema.ts",
  "output": "Sources/App/Schema.swift",
  "schemaName": "app",
  "tables": {
    "issue": {"columns": {"shortID": {"type": "int"}}}
  }
}
```

`number()` columns are `Double`. Set a column's type to `int` to make it an
`Int`; it must then hold integers. For a `json<T>()` column whose `T` is a
plain object or a literal union, the tool generates a `Codable` type.

`--schema`, `--out`, and `--config` override the config file.

## `zero-swift drift record`

You write the Swift queries and mutators by hand, and the drift harness
checks them against the TypeScript ones. This command runs the TypeScript
definitions on samples and records what they did. `Drift.failures` in
the Swift `ZeroTesting` library then lists wherever the Swift ones differ,
for a test to expect none.

```sh
npx zero-swift drift record --samples drift-samples.ts --out Tests/AppTests/drift-vectors.json
```

The samples module's default export holds:

- `schema`
- the `queries` and `mutators` registries
- `querySamples`, each a name with optional `args` and `context`
- `mutatorSamples`, each also with optional `seed` rows

Record again whenever either side changes. `--check` fails when the recorded
file is out of date.
