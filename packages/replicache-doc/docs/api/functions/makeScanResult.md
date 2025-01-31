# Function: makeScanResult()

> **makeScanResult**\<`Options`\>(`options`, `getScanIterator`): [`ScanResult`](../interfaces/ScanResult.md)\<[`KeyTypeForScanOptions`](../type-aliases/KeyTypeForScanOptions.md)\<`Options`\>, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

A helper function that makes it easier to implement [ReadTransaction.scan](../interfaces/ReadTransaction.md#scan)
with a custom backend.

If you are implementing a custom backend and have an in memory pending async
iterable we provide two helper functions to make it easier to merge these
together. [mergeAsyncIterables](mergeAsyncIterables.md) and [filterAsyncIterable](filterAsyncIterable.md).

For example:

```ts
const scanResult = makeScanResult(
  options,
  options.indexName
    ? () => {
        throw Error('not implemented');
      }
    : fromKey => {
        const persisted: AsyncIterable<Entry<ReadonlyJSONValue>> = ...;
        const pending: AsyncIterable<Entry<ReadonlyJSONValue | undefined>> = ...;
        const iter = await mergeAsyncIterables(persisted, pending);
        const filteredIter = await filterAsyncIterable(
          iter,
          entry => entry[1] !== undefined,
        );
        return filteredIter;
      },
);
```

## Type Parameters

• **Options** *extends* [`ScanOptions`](../type-aliases/ScanOptions.md)

## Parameters

### options

`Options`

### getScanIterator

`Options` *extends* [`ScanIndexOptions`](../type-aliases/ScanIndexOptions.md) ? [`GetIndexScanIterator`](../type-aliases/GetIndexScanIterator.md) : [`GetScanIterator`](../type-aliases/GetScanIterator.md)

## Returns

[`ScanResult`](../interfaces/ScanResult.md)\<[`KeyTypeForScanOptions`](../type-aliases/KeyTypeForScanOptions.md)\<`Options`\>, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>
