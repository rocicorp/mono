# Type Alias: PatchOperation

> **PatchOperation**: \{ `key`: `string`; `op`: `"put"`; `value`: [`ReadonlyJSONValue`](ReadonlyJSONValue.md); \} \| \{ `key`: `string`; `op`: `"del"`; \} \| \{ `op`: `"clear"`; \}

This type describes the patch field in a [PullResponse](PullResponse.md) and it is used
to describe how to update the Replicache key-value store.
