# Interface: ScanResult\<K, V\>

## Extends

- `AsyncIterable`\<`V`\>

## Type Parameters

• **K** *extends* `ScanKey`

• **V**

## Methods

### \[asyncIterator\]()

> **\[asyncIterator\]**(): [`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`V`\>

The default AsyncIterable. This is the same as [values](ScanResult.md#values).

#### Returns

[`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`V`\>

#### Overrides

`AsyncIterable.[asyncIterator]`

***

### entries()

> **entries**(): [`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<readonly \[`K`, `V`\]\>

Async iterator over the entries of the [scan](ReadTransaction.md#scan)
call. An entry is a tuple of key values. If the
[scan](ReadTransaction.md#scan) is over an index the key is a tuple of
`[secondaryKey: string, primaryKey]`

#### Returns

[`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<readonly \[`K`, `V`\]\>

***

### keys()

> **keys**(): [`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`K`\>

Async iterator over the keys of the [scan](ReadTransaction.md#scan)
call. If the [scan](ReadTransaction.md#scan) is over an index the key
is a tuple of `[secondaryKey: string, primaryKey]`

#### Returns

[`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`K`\>

***

### toArray()

> **toArray**(): `Promise`\<`V`[]\>

Returns all the values as an array. Same as `values().toArray()`

#### Returns

`Promise`\<`V`[]\>

***

### values()

> **values**(): [`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`V`\>

Async iterator over the values of the [scan](ReadTransaction.md#scan) call.

#### Returns

[`AsyncIterableIteratorToArray`](AsyncIterableIteratorToArray.md)\<`V`\>
