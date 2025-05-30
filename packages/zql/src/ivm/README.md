# ZVM

```ts
const s = new Source();

s.connect(
  sort, // optional. For index selection for hydration.
  filters, // optional. For index selection for hydration.
  splitEditKeys,
);
```

# TODO:

1. New `sort` operator
2. New PipelineBuilder API
3. Range jump in `memory-source` if filter matches prefix of order
4. Docs, examples
5. Indexed filters to skip pipelines on push

(5) is an optimization that sources are responsible for implementing.
