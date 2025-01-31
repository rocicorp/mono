# Type Alias: DropDatabaseOptions

> **DropDatabaseOptions**: `object`

Options for `dropDatabase` and `dropAllDatabases`.

## Type declaration

### kvStore?

> `optional` **kvStore**: `"idb"` \| `"mem"` \| [`KVStoreProvider`](KVStoreProvider.md)

Allows providing a custom implementation of the underlying storage layer.
Default is `'idb'`.

### logLevel?

> `optional` **logLevel**: [`LogLevel`](LogLevel.md)

Determines how much logging to do. When this is set to `'debug'`,
Replicache will also log `'info'` and `'error'` messages. When set to
`'info'` we log `'info'` and `'error'` but not `'debug'`. When set to
`'error'` we only log `'error'` messages.
Default is `'info'`.

### logSinks?

> `optional` **logSinks**: [`LogSink`](../interfaces/LogSink.md)[]

Enables custom handling of logs.

By default logs are logged to the console.  If you would like logs to be
sent elsewhere (e.g. to a cloud logging service like DataDog) you can
provide an array of [LogSink](../interfaces/LogSink.md)s.  Logs at or above
[DropDatabaseOptions.logLevel](DropDatabaseOptions.md#loglevel) are sent to each of these [LogSink](../interfaces/LogSink.md)s.
If you would still like logs to go to the console, include
`consoleLogSink` in the array.

```ts
logSinks: [consoleLogSink, myCloudLogSink],
```
Default is `[consoleLogSink]`.
