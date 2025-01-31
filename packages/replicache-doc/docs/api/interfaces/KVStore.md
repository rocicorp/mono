# Interface: KVStore

Store defines a transactional key/value store that Replicache stores all data
within.

For correct operation of Replicache, implementations of this interface must
provide [strict
serializable](https://jepsen.io/consistency/models/strict-serializable)
transactions.

Informally, read and write transactions must behave like a ReadWrite Lock -
multiple read transactions are allowed in parallel, or one write.
Additionally writes from a transaction must appear all at one, atomically.

## Properties

### closed

> **closed**: `boolean`

## Methods

### close()

> **close**(): `Promise`\<`void`\>

#### Returns

`Promise`\<`void`\>

***

### read()

> **read**(): `Promise`\<[`KVRead`](KVRead.md)\>

#### Returns

`Promise`\<[`KVRead`](KVRead.md)\>

***

### write()

> **write**(): `Promise`\<[`KVWrite`](KVWrite.md)\>

#### Returns

`Promise`\<[`KVWrite`](KVWrite.md)\>
