# Type Alias: ScanIndexOptions

> **ScanIndexOptions**: `object`

Options for [scan](../interfaces/ReadTransaction.md#scan) when scanning over an index. When
scanning over and index you need to provide the `indexName` and the `start`
`key` is now a tuple consisting of secondary and primary key

## Type declaration

### indexName

> **indexName**: `string`

Do a [scan](../interfaces/ReadTransaction.md#scan) over a named index. The `indexName` is
the name of an index defined when creating the [Replicache](../classes/Replicache.md) instance using
[ReplicacheOptions.indexes](../interfaces/ReplicacheOptions.md#indexes).

### limit?

> `optional` **limit**: `number`

Only include up to `limit` results.

### prefix?

> `optional` **prefix**: `string`

Only include results starting with the *secondary* keys starting with `prefix`.

### start?

> `optional` **start**: `object`

When provided the scan starts at this key.

#### start.exclusive?

> `optional` **exclusive**: `boolean`

Whether the `key` is exclusive or inclusive.

#### start.key

> **key**: [`ScanOptionIndexedStartKey`](ScanOptionIndexedStartKey.md)
