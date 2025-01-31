# Type Alias: ExperimentalWatchNoIndexOptions

> **ExperimentalWatchNoIndexOptions**: `object`

Options object passed to [Replicache.experimentalWatch](../classes/Replicache.md#experimentalwatch). This is for a non
index watch.

## Type declaration

### initialValuesInFirstDiff?

> `optional` **initialValuesInFirstDiff**: `boolean`

When this is set to `true` (default is `false`), the `watch` callback will
be called once asynchronously when watch is called. The arguments in that
case is a diff where we consider all the existing values in Replicache as
being added.

### prefix?

> `optional` **prefix**: `string`

When provided, the `watch` is limited to changes where the `key` starts
with `prefix`.
