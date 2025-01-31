# Interface: ReadTransaction

ReadTransactions are used with [Replicache.query](../classes/Replicache.md#query) and
[Replicache.subscribe](../classes/Replicache.md#subscribe) and allows read operations on the
database.

## Extended by

- [`WriteTransaction`](WriteTransaction.md)

## Properties

### clientID

> `readonly` **clientID**: `string`

***

### ~~environment~~

> `readonly` **environment**: [`TransactionEnvironment`](../type-aliases/TransactionEnvironment.md)

#### Deprecated

Use [ReadTransaction.location](ReadTransaction.md#location) instead.

***

### location

> `readonly` **location**: [`TransactionEnvironment`](../type-aliases/TransactionEnvironment.md)

## Methods

### get()

#### Call Signature

> **get**(`key`): `Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

Get a single value from the database. If the `key` is not present this
returns `undefined`.

Important: The returned JSON is readonly and should not be modified. This
is only enforced statically by TypeScript and there are no runtime checks
for performance reasons. If you mutate the return value you will get
undefined behavior.

##### Parameters

###### key

`string`

##### Returns

`Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

#### Call Signature

> **get**\<`T`\>(`key`): `Promise`\<`undefined` \| [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`T`\>\>

##### Type Parameters

• **T** *extends* [`JSONValue`](../type-aliases/JSONValue.md)

##### Parameters

###### key

`string`

##### Returns

`Promise`\<`undefined` \| [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`T`\>\>

***

### has()

> **has**(`key`): `Promise`\<`boolean`\>

Determines if a single `key` is present in the database.

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`boolean`\>

***

### isEmpty()

> **isEmpty**(): `Promise`\<`boolean`\>

Whether the database is empty.

#### Returns

`Promise`\<`boolean`\>

***

### scan()

#### Call Signature

> **scan**(`options`): [`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

Gets many values from the database. This returns a [ScanResult](ScanResult.md) which
implements `AsyncIterable`. It also has methods to iterate over the
[keys](ScanResult.md#keys) and [entries](ScanResult.md#entries).

If `options` has an `indexName`, then this does a scan over an index with
that name. A scan over an index uses a tuple for the key consisting of
`[secondary: string, primary: string]`.

If the [ScanResult](ScanResult.md) is used after the `ReadTransaction` has been closed
it will throw a [TransactionClosedError](../classes/TransactionClosedError.md).

Important: The returned JSON is readonly and should not be modified. This
is only enforced statically by TypeScript and there are no runtime checks
for performance reasons. If you mutate the return value you will get
undefined behavior.

##### Parameters

###### options

[`ScanIndexOptions`](../type-aliases/ScanIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

#### Call Signature

> **scan**(`options`?): [`ScanResult`](ScanResult.md)\<`string`, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Parameters

###### options?

[`ScanNoIndexOptions`](../type-aliases/ScanNoIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string`, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

#### Call Signature

> **scan**(`options`?): [`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Parameters

###### options?

[`ScanOptions`](../type-aliases/ScanOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

#### Call Signature

> **scan**\<`V`\>(`options`): [`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options

[`ScanIndexOptions`](../type-aliases/ScanIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

#### Call Signature

> **scan**\<`V`\>(`options`?): [`ScanResult`](ScanResult.md)\<`string`, [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options?

[`ScanNoIndexOptions`](../type-aliases/ScanNoIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string`, [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

#### Call Signature

> **scan**\<`V`\>(`options`?): [`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options?

[`ScanOptions`](../type-aliases/ScanOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>
