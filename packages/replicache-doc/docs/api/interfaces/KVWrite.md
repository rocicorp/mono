# Interface: KVWrite

**`Experimental`**

This interface is experimental and might be removed or changed
in the future without following semver versioning. Please be cautious.

## Extends

- [`KVRead`](KVRead.md)

## Properties

### closed

> **closed**: `boolean`

**`Experimental`**

#### Inherited from

[`KVRead`](KVRead.md).[`closed`](KVRead.md#closed)

## Methods

### commit()

> **commit**(): `Promise`\<`void`\>

**`Experimental`**

#### Returns

`Promise`\<`void`\>

***

### del()

> **del**(`key`): `Promise`\<`void`\>

**`Experimental`**

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`void`\>

***

### get()

> **get**(`key`): `Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

**`Experimental`**

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

#### Inherited from

[`KVRead`](KVRead.md).[`get`](KVRead.md#get)

***

### has()

> **has**(`key`): `Promise`\<`boolean`\>

**`Experimental`**

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`boolean`\>

#### Inherited from

[`KVRead`](KVRead.md).[`has`](KVRead.md#has)

***

### put()

> **put**(`key`, `value`): `Promise`\<`void`\>

**`Experimental`**

#### Parameters

##### key

`string`

##### value

[`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)

#### Returns

`Promise`\<`void`\>

***

### release()

> **release**(): `void`

**`Experimental`**

#### Returns

`void`

#### Inherited from

[`KVRead`](KVRead.md).[`release`](KVRead.md#release)
