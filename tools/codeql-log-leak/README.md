# Zero log-leak CodeQL example

`ZeroLogLeak.ql` is a starting point for replacing the LLM review in PR #6411
with deterministic global taint analysis. It models:

- Zero rows, query ASTs, mutation data, authentication data, and PostgreSQL
  diagnostics as sources.
- `LogContext`, `LogSink`, `console`, `Error` construction, and `throw` as
  sinks.
- Stringification and error wrappers as taint-preserving operations.
- Safe metadata fields and `safe()`, `safe.count()`, `safe.hash()`, and
  `safe.shape()` as barriers.

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

The query intentionally starts with a small, explicit model. Before making it
a blocking check, run it against known leaking and allowed examples and adjust:

1. The module-name patterns used to resolve local TypeScript types.
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

CodeQL should analyze the complete checkout in CI. A PR check can then filter
the SARIF results to alerts whose sink is on a changed line. This still detects
a changed source or propagator that reaches an existing sink.
