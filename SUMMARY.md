## Query pipeline rename and symmetry

- Aligned the query layers to mirror mutators: `QueryDefinition` → `Query` (args setter with `fn({ctx,args})`) → `QueryRequest` (args + query). The fluent `Query` builder was renamed to `QueryBuilder` and now implements `toZQL`.
- Renamed `toQuery` to `toZQL`.
- Updated the registry to construct the new `Query`/`QueryRequest` shapes, tag phantoms with `$hasArgs`, and rename `createCustomQueryBuilder`.
- Propagated the new names through builders, schema query, zero client APIs, and React/Solid hooks; ensured runnable/abstract query classes implement `toZQL`.

### TODO

So naming is mostly parallel (Definition/Definitions/Registry/To*/From* and define* functions). The main divergences
 are (1) CustomQuery vs Mutator, (2) query registries use a string tag while mutator registries use a symbol, and (3)
  queries expose extra QueryTypes/QueryRegistryTypes phantoms whereas mutators stop at MutatorDefinitionTypes.
