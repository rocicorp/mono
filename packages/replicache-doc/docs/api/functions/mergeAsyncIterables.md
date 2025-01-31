# Function: mergeAsyncIterables()

> **mergeAsyncIterables**\<`A`, `B`\>(`iterableBase`, `iterableOverlay`, `compare`): `AsyncIterable`\<`A` \| `B`\>

Merges an iterable on to another iterable.

The two iterables need to be ordered and the `compare` function is used to
compare two different elements.

If two elements are equal (`compare` returns `0`) then the element from the
second iterable is picked.

This utility function is provided because it is useful when using
[makeScanResult](makeScanResult.md). It can be used to merge an in memory pending async
iterable on to a persistent async iterable for example.

## Type Parameters

• **A**

• **B**

## Parameters

### iterableBase

[`IterableUnion`](../type-aliases/IterableUnion.md)\<`A`\>

### iterableOverlay

[`IterableUnion`](../type-aliases/IterableUnion.md)\<`B`\>

### compare

(`a`, `b`) => `number`

## Returns

`AsyncIterable`\<`A` \| `B`\>
