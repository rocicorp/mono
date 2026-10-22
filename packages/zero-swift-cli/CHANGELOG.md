# zero-swift

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
