## Query pipeline rename and symmetry

- Aligned the query layers to mirror mutators: `QueryDefinition` → `Query` (args setter with `fn({ctx,args})`) → `QueryRequest` (args + query).
- The fluent `Query` builder was renamed to `QueryBuilder`.
- Renamed `toQuery` to `toZQL`. Only QueryRequest implements this - `QueryBuilder` no longer extends `toZQL`.
- All query intro points now take `QueryRequestOrBuilder` which either injects the context or runs the query as-is.
- Updated the registry to construct the new `Query`/`QueryRequest` shapes.
- Propagated the new names through builders, schema query, zero client APIs, and React/Solid hooks.
