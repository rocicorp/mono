Review only added or modified lines for customer application data entering logs
or thrown `Error` messages. Ignore style, naming, tests, performance, and other
correctness concerns.

## Block

Block values originating from:

- Customer rows (`Row`, `RowValue`, Zero `RowList`, `InsertOp`, `UpdateOp`,
  `DeleteOp`, `CRUDOp`, `row`, `rows`, `newRow`, `oldRow`, `msg.new`, `msg.old`).
- Query literals or bound parameters (`AST`, `Condition`, `LiteralValue`,
  `q.ast`, `q.details`, `q.args`, or any `.parameters`).
- Authentication or mutations (`JWTPayload`, `authData`, decoded tokens,
  mutation arguments, custom-mutator payloads, `op.value`).
- Postgres `Notice` and `PostgresError` objects or their `message`, `detail`,
  `hint`, and `where` fields. These can contain interpolated row values.

Also block app data embedded at a `new Error(...)` or `throw` site; it may be
logged later as a plain `Error`.

Trace values through up to three wrappers, including interpolation,
`JSON.stringify`, `stringify`, object spread, `toErrorLogObject`, `String`,
`toString`, `util.inspect`, and `%o`/`%j` arguments.

## Allow

- A log or throw expression immediately preceded by `// log-leak-ignore` or
  `// log-leak-ignore -- reason`. The annotation approves only that next
  expression, including a multiline expression.
- Table and column names or types, including `clientSchema`, `columnSpec`,
  `tableName`, column-name lists, and `op.primaryKey` (names, not values).
- Parameterized SQL text such as `query.string` and `q.string`; parameters are
  not safe.
- postgres.js rows from infrastructure queries: replication slots, `SHOW`, and
  catalog queries in `replication-slots.ts`, `change-source.ts`, and
  `cvr-purger.ts`. Distinguish these from Zero rows.
- OTel diagnostic metadata in `server/otel-diag-logger.ts`.
- Counts, durations, IDs, booleans, log levels, component names, hashes, and
  values wrapped in `safe()`, `safe.count()`, `safe.hash()`, or `safe.shape()`.

Use Read, Grep, and Glob to resolve a logged expression's source and static
type. If it remains ambiguous, warn instead of blocking.

## Output

The first line must be exactly one of:

    VERDICT: PASS
    VERDICT: WARN
    VERDICT: BLOCK

For PASS, output only the verdict. Otherwise add one line per finding, most
severe first:

    BLOCK path/file.ts:123 | expression :: type | leaked data | fix
    WARN  path/file.ts:456 | expression :: type | uncertainty | what to check

Use no preamble, summary, or markdown. Keep every field to one clause.
