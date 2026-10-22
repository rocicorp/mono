---
'zero-swift': patch
---

Calling a mutator that isn't registered with the `Zero` no longer crashes the app: `mutate` logs an error and returns a result whose `client` and `server` are both a `.zeroError` with the message JS throws, and nothing is written or sent. Waiting for `.complete` or `.cached` on an unnamed query, or preloading one, now logs a warning once per query: an unnamed query only sees local data and is never complete, so use a named query to sync it. The docs of `run` and `preload` say what happens to a pending call when the client closes or is replaced. `zero-swift codegen --help` no longer prints the examples `--schemaFile shared/schema.ts` and `--check true`; it shows `--schema-file` and a bare `--check`.
