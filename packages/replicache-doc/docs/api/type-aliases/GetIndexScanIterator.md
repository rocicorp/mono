# Type Alias: GetIndexScanIterator()

> **GetIndexScanIterator**: (`indexName`, `fromSecondaryKey`, `fromPrimaryKey`) => [`IterableUnion`](IterableUnion.md)\<readonly \[[`IndexKey`](IndexKey.md), [`ReadonlyJSONValue`](ReadonlyJSONValue.md)\]\>

When using [makeScanResult](../functions/makeScanResult.md) this is the type used for the function called when doing a [scan](../interfaces/ReadTransaction.md#scan) with an
`indexName`.

## Parameters

### indexName

`string`

The name of the index we are scanning over.

### fromSecondaryKey

`string`

The `fromSecondaryKey` is computed by `scan` and is
the secondary key of the first entry to return in the iterator. It is based
on `prefix` and `start.key` of the [ScanIndexOptions](ScanIndexOptions.md).

### fromPrimaryKey

The `fromPrimaryKey` is computed by `scan` and is the
primary key of the first entry to return in the iterator. It is based on
`prefix` and `start.key` of the [ScanIndexOptions](ScanIndexOptions.md).

`string` | `undefined`

## Returns

[`IterableUnion`](IterableUnion.md)\<readonly \[[`IndexKey`](IndexKey.md), [`ReadonlyJSONValue`](ReadonlyJSONValue.md)\]\>
