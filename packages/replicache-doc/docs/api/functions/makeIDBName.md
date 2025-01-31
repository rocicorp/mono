# Function: makeIDBName()

> **makeIDBName**(`name`, `schemaVersion`?): `string`

Returns the name of the IDB database that will be used for a particular Replicache instance.

## Parameters

### name

`string`

The name of the Replicache instance (i.e., the `name` field of `ReplicacheOptions`).

### schemaVersion?

`string`

The schema version of the database (i.e., the `schemaVersion` field of `ReplicacheOptions`).

## Returns

`string`
