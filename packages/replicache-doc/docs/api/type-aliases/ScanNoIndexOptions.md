# Type Alias: ScanNoIndexOptions

> **ScanNoIndexOptions**: `object`

Options for [scan](../interfaces/ReadTransaction.md#scan) when scanning over the entire key
space.

## Type declaration

### limit?

> `optional` **limit**: `number`

Only include up to `limit` results.

### prefix?

> `optional` **prefix**: `string`

Only include keys starting with `prefix`.

### start?

> `optional` **start**: `object`

When provided the scan starts at this key.

#### start.exclusive?

> `optional` **exclusive**: `boolean`

Whether the `key` is exclusive or inclusive.

#### start.key

> **key**: `string`
