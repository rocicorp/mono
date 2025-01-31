# Function: dropAllDatabases()

> **dropAllDatabases**(`opts`?): `Promise`\<\{ `dropped`: `string`[]; `errors`: `unknown`[]; \}\>

Deletes all IndexedDB data associated with Replicache.

Returns an object with the names of the successfully dropped databases
and any errors encountered while dropping.

## Parameters

### opts?

[`DropDatabaseOptions`](../type-aliases/DropDatabaseOptions.md)

## Returns

`Promise`\<\{ `dropped`: `string`[]; `errors`: `unknown`[]; \}\>
