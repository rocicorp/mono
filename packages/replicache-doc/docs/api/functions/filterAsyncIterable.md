# Function: filterAsyncIterable()

> **filterAsyncIterable**\<`V`\>(`iter`, `predicate`): `AsyncIterable`\<`V`\>

Filters an async iterable.

This utility function is provided because it is useful when using
[makeScanResult](makeScanResult.md). It can be used to filter out tombstones (delete entries)
for example.

## Type Parameters

• **V**

## Parameters

### iter

[`IterableUnion`](../type-aliases/IterableUnion.md)\<`V`\>

### predicate

(`v`) => `boolean`

## Returns

`AsyncIterable`\<`V`\>
