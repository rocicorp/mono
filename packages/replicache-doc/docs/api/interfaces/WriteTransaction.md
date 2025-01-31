# Interface: WriteTransaction

WriteTransactions are used with *mutators* which are registered using
[ReplicacheOptions.mutators](ReplicacheOptions.md#mutators) and allows read and write operations on the
database.

## Extends

- [`ReadTransaction`](ReadTransaction.md)

## Properties

### clientID

> `readonly` **clientID**: `string`

#### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`clientID`](ReadTransaction.md#clientid)

***

### ~~environment~~

> `readonly` **environment**: [`TransactionEnvironment`](../type-aliases/TransactionEnvironment.md)

#### Deprecated

Use [ReadTransaction.location](ReadTransaction.md#location) instead.

#### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`environment`](ReadTransaction.md#environment)

***

### location

> `readonly` **location**: [`TransactionEnvironment`](../type-aliases/TransactionEnvironment.md)

#### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`location`](ReadTransaction.md#location)

***

### mutationID

> `readonly` **mutationID**: `number`

The ID of the mutation that is being applied.

***

### reason

> `readonly` **reason**: [`TransactionReason`](../type-aliases/TransactionReason.md)

The reason for the transaction. This can be `initial`, `rebase` or `authoriative`.

## Methods

### del()

> **del**(`key`): `Promise`\<`boolean`\>

Removes a `key` and its value from the database. Returns `true` if there was a
`key` to remove.

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`boolean`\>

***

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

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`get`](ReadTransaction.md#get)

#### Call Signature

> **get**\<`T`\>(`key`): `Promise`\<`undefined` \| [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`T`\>\>

##### Type Parameters

• **T** *extends* [`JSONValue`](../type-aliases/JSONValue.md)

##### Parameters

###### key

`string`

##### Returns

`Promise`\<`undefined` \| [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`T`\>\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`get`](ReadTransaction.md#get)

***

### has()

> **has**(`key`): `Promise`\<`boolean`\>

Determines if a single `key` is present in the database.

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`boolean`\>

#### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`has`](ReadTransaction.md#has)

***

### isEmpty()

> **isEmpty**(): `Promise`\<`boolean`\>

Whether the database is empty.

#### Returns

`Promise`\<`boolean`\>

#### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`isEmpty`](ReadTransaction.md#isempty)

***

### ~~put()~~

> **put**(`key`, `value`): `Promise`\<`void`\>

#### Parameters

##### key

`string`

##### value

[`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

#### Returns

`Promise`\<`void`\>

#### Deprecated

Use [WriteTransaction.set](WriteTransaction.md#set) instead.

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

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

#### Call Signature

> **scan**(`options`?): [`ScanResult`](ScanResult.md)\<`string`, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Parameters

###### options?

[`ScanNoIndexOptions`](../type-aliases/ScanNoIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string`, [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

#### Call Signature

> **scan**(`options`?): [`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Parameters

###### options?

[`ScanOptions`](../type-aliases/ScanOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

#### Call Signature

> **scan**\<`V`\>(`options`): [`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options

[`ScanIndexOptions`](../type-aliases/ScanIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<[`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

#### Call Signature

> **scan**\<`V`\>(`options`?): [`ScanResult`](ScanResult.md)\<`string`, [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options?

[`ScanNoIndexOptions`](../type-aliases/ScanNoIndexOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string`, [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

#### Call Signature

> **scan**\<`V`\>(`options`?): [`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Type Parameters

• **V** *extends* [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

##### Parameters

###### options?

[`ScanOptions`](../type-aliases/ScanOptions.md)

##### Returns

[`ScanResult`](ScanResult.md)\<`string` \| [`IndexKey`](../type-aliases/IndexKey.md), [`DeepReadonly`](../type-aliases/DeepReadonly.md)\<`V`\>\>

##### Inherited from

[`ReadTransaction`](ReadTransaction.md).[`scan`](ReadTransaction.md#scan)

***

### set()

> **set**(`key`, `value`): `Promise`\<`void`\>

Sets a single `value` in the database. The value will be frozen (using
`Object.freeze`) in debug mode.

#### Parameters

##### key

`string`

##### value

[`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

#### Returns

`Promise`\<`void`\>
