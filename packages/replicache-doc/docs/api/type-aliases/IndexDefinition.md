# Type Alias: IndexDefinition

> **IndexDefinition**: `object`

The definition of a single index.

## Type declaration

### allowEmpty?

> `readonly` `optional` **allowEmpty**: `boolean`

If `true`, indexing empty values will not emit a warning.  Defaults to `false`.

### jsonPointer

> `readonly` **jsonPointer**: `string`

A [JSON Pointer](https://tools.ietf.org/html/rfc6901) pointing at the sub
value inside each value to index over.

For example, one might index over users' ages like so:
`{prefix: '/user/', jsonPointer: '/age'}`

### prefix?

> `readonly` `optional` **prefix**: `string`

The prefix, if any, to limit the index over. If not provided the values of
all keys are indexed.
