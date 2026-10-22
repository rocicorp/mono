# zero-swift

## 0.4.0

### Minor Changes

- 76312e4: `JSONValue.object` holds a `JSONObject` instead of a `[String: JSONValue]`. A `JSONObject` keeps its keys as a JS object does: integer-like keys first in ascending order, then the others in the order they were set, so JSON is written as `JSON.stringify` writes it. Keys are compared by UTF-16 code unit, so keys that differ only in Unicode normalization (`"é"` and `"e\u{301}"`) stay apart instead of one overwriting the other. Before, they merged in pokes (row values, primary keys, merges, client IDs), and custom-query and AST hashes of such keys differed from JS. Dictionary literals, `object[key]`, and `for (key, value) in object` work as before, and `Row(_:)` and `Row.object` convert both ways.

  Migration: where code builds `.object(dict)` from a `[String: JSONValue]` variable, write `.object(JSONObject(dict))` or declare the variable as `JSONObject`. `objectValue` and `ZeroObjectReader.object`/`ZeroObjectWriter.object` are now `JSONObject`; use `.dictionary` where a `[String: JSONValue]` is needed. `AST.Bound.row` is a `JSONObject`, and `AnyQuery.start`, `TableMutator`, and `LoopbackServer.put` take a `JSONObject` as well as a dictionary.

### Patch Changes

- fcc95f0: Two `Zero` instances of one client group in one process (the same user, `storageKey`, and storage, such as two windows) no longer drop each other's queries: `initConnection` lists every running client of the group in `activeClients`, as in JS, so zero-cache doesn't delete the other one. When one closes, the others send `deleteClients` for it once.
- 48ca692: A first poke that changes nothing after a reconnect or relaunch completes the got queries, as in JS. `<=` and `>=` filters no longer match `NaN`. Storage: every client sharing a database hears about a write failure, and a closed client doesn't; writes queued before a failure no longer land after it; a database busy past the busy timeout is retried by the next write instead of being a storage failure; a read failure loading the client group is reported through `onStorageFailure` and moves the client to memory; and the memory fallback after a failed open is shared by name in the process, like `.memory`.
- a548803: `codegen` generates Swift that compiles for any table, column, relationship, JSON field, or enumeration value name. A name that is taken, a keyword Swift doesn't allow even backticked, a member the generated code declares, or a type it uses is renamed by a documented rule, and keeps its original name in the schema, `CodingKeys`, and raw values. Names outside ASCII are kept where Swift allows them, enumeration values with control characters are escaped as Swift allows, and a recursive `json<T>()` type is `JSONValue` where it recurs instead of overflowing the stack.

## 0.3.0

### Minor Changes

- 8a69a95: Depending on zero-swift no longer fetches swift-nio, swift-atomics, swift-collections, and swift-system. The `ZeroNIOTesting` product is removed: its `NIOTransport`, a plain `ws://` transport for Linux, is now part of zero-swift's own end-to-end tests.
- e446e70: `zero.delete()` closes the client and deletes its local data: the replica and pending mutations of its user and `storageKey`, as in JS. `dropAllDatabases(_:)` deletes every Zero database in a storage location. Both return the `DroppedDatabases`.

### Patch Changes

- 9aec532: Cancelling the task of a `zero.run(_:type: .complete)` or `.cached` makes it throw `CancellationError` and release its query; before, it kept waiting. The `.zeroError` mutation outcome now documents that the server may still have applied the mutation, so it isn't safe to retry.

  ZeroVirtual has a DocC catalog: restoring each list's scroll position and selection, selection in a compact `NavigationSplitView`, and making `VirtualListView` rows accessibility elements. Getting Started covers leaving `queryURL` and `mutateURL` unset, and developing against a local backend: sandboxed Mac apps, the iOS simulator, physical devices, and zero-cache's port.

## 0.2.0

### Minor Changes

- c7b53df: The CLI is built on [incur](https://github.com/wevm/incur). Errors print a code and a message and exit 1. `--json` prints results as JSON, `--llms` prints a manifest for coding agents, and `skills add` installs the commands as an agent skill. `codegen` has an option for every config key, such as `--type-prefix` and `--access`. The schema module option is now `--schema-file`, because incur reserves `--schema`; `--schema <path>` still works. The config keys are now documented in the README.

## 0.1.1

### Patch Changes

- 5251897: `zero-swift --help` prints usage, and a missing schema, samples, or config file or an unknown option gets a one-line message instead of a stack trace.
