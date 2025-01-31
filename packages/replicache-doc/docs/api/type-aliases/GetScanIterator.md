# Type Alias: GetScanIterator()

> **GetScanIterator**: (`fromKey`) => [`IterableUnion`](IterableUnion.md)\<`Entry`\<[`ReadonlyJSONValue`](ReadonlyJSONValue.md)\>\>

This is called when doing a [scan](../interfaces/ReadTransaction.md#scan) without an
`indexName`.

## Parameters

### fromKey

`string`

The `fromKey` is computed by `scan` and is the key of the
first entry to return in the iterator. It is based on `prefix` and
`start.key` of the [ScanNoIndexOptions](ScanNoIndexOptions.md).

## Returns

[`IterableUnion`](IterableUnion.md)\<`Entry`\<[`ReadonlyJSONValue`](ReadonlyJSONValue.md)\>\>
