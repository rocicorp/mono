# Type Alias: CreateKVStore()

> **CreateKVStore**: (`name`) => [`KVStore`](../interfaces/KVStore.md)

Factory function for creating [Store](../interfaces/KVStore.md) instances.

The name is used to identify the store. If the same name is used for multiple
stores, they should share the same data. It is also desirable to have these
stores share an RWLock.

## Parameters

### name

`string`

## Returns

[`KVStore`](../interfaces/KVStore.md)
