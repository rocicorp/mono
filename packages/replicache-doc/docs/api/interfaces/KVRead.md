# Interface: KVRead

**`Experimental`**

This interface is experimental and might be removed or changed
in the future without following semver versioning. Please be cautious.

## Extends

- `Release`

## Extended by

- [`KVWrite`](KVWrite.md)

## Properties

### closed

> **closed**: `boolean`

**`Experimental`**

## Methods

### get()

> **get**(`key`): `Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

**`Experimental`**

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`undefined` \| [`ReadonlyJSONValue`](../type-aliases/ReadonlyJSONValue.md)\>

***

### has()

> **has**(`key`): `Promise`\<`boolean`\>

**`Experimental`**

#### Parameters

##### key

`string`

#### Returns

`Promise`\<`boolean`\>

***

### release()

> **release**(): `void`

**`Experimental`**

#### Returns

`void`

#### Inherited from

`Release.release`
