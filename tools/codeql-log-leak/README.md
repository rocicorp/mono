# Zero log-leak CodeQL example

`ZeroLogLeak.ql` is a starting point for replacing the LLM review in PR #6411
with deterministic global taint analysis. It models:

- Zero rows, query ASTs, mutation data, authentication data, and PostgreSQL
  diagnostics as sources.
- `LogContext`, `LogSink`, `console`, `Error` construction, and the arguments
  the assertion helpers in `shared/asserts.ts` actually throw as sinks: the
  message of `assert`, and the value handed to the `assertX` family, which
  interpolates it. `unreachable` and `assertNotNull` throw fixed strings and
  are not sinks. A bare `throw` is not a
  sink: rethrowing an error discloses nothing the construction site did not
  already, and treating it as one reported every `throw e` in the tree.
- Stringification and error wrappers as taint-preserving operations.
- Safe metadata fields, counts (`.length`, `.size`), caught exceptions, and
  `safe()`, `safe.count()`, `safe.hash()`, and `safe.shape()` as barriers.
  Safe fields match on the property name alone: requiring a sensitive base
  type would switch the exemption off as soon as a value had passed through a
  loop or a call.
- Tests, benchmarks, CLI entry points, and the throughput harness as
  non-production code, since printing queries is what those exist to do.

## Run the query

Install the CodeQL CLI, then run these commands from the repository root:

```bash
codeql pack install tools/codeql-log-leak
codeql database create /tmp/zero-codeql-db \
  --language=javascript-typescript \
  --source-root=.
codeql database analyze /tmp/zero-codeql-db \
  tools/codeql-log-leak \
  --format=sarif-latest \
  --output=/tmp/zero-log-leak.sarif
```

## How types are matched

CodeQL records a module name only for bare package specifiers such as
`postgres` or `@rocicorp/logger`; an import whose path starts with `.` or `/`
gets none at all. Since the Zero types are imported by relative path, they are
invisible to `hasUnderlyingType(module, type)`, and are matched on their type
annotation instead: the annotation names one of the listed types and its
declaration resolves to a file in this repository. Package types such as
`jose.JWTPayload` still go through `hasUnderlyingType`.

That trades away what the type checker knows. A value the annotation route
misses is one that was never annotated -- an inferred call result, for
instance. Unions, aliases, optionals, and arrays are unwrapped, so
`Row | undefined` and `RowValue[]` do match.

The query intentionally starts with a small, explicit model. Before making it
a blocking check, run it against known leaking and allowed examples and adjust:

1. The list of sensitive type names.
2. The safe metadata property list.
3. Project-specific logging wrappers.
4. The `safe` helper model after that helper has a permanent module path.

## Suppressing an alert

Mark the log call with `log-leak-ignore`, either at the end of its own line or
on the line directly above it:

```ts
// log-leak-ignore
lc.debug?.(`${q.string} (${q.parameters.length} params)`);
```

Suppression is line-based, not scope-based, so a later edit that adds another
value to the same call does not silently inherit the exemption. Reach for it
when a value is safe for a reason the query cannot see; when the reason
generalizes -- a count, a hash, a shape -- teach the query a barrier instead.

`summarize-sarif.ts` renders the results and decides whether the run passes;
Node runs it directly, as elsewhere in the repo.

CodeQL analyzes the complete checkout in CI, via `.github/workflows/codeql.yml`.
Two checks report on it, and they are not redundant: GitHub's own code scanning
check reports only alerts in code a pull request changed, while the workflow's
`Check alerts` step reads the whole SARIF and fails on any alert, so a
pre-existing leak cannot pass unnoticed.
