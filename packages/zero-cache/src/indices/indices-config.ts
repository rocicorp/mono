import * as v from '../../../shared/src/valita.ts';

const fulltextIndexSchema = v.object({
  columns: v.array(v.string()),
  tokenizer: v.string().optional(),
});

export type FulltextIndex = v.Infer<typeof fulltextIndexSchema>;

const tableIndicesSchema = v.object({
  fulltext: v.array(fulltextIndexSchema).optional(),
  // Future: other index types can be added here
  // e.g., denormalized: v.array(...).optional()
});

export type TableIndices = v.Infer<typeof tableIndicesSchema>;

export const indicesConfigSchema = v.object({
  tables: v.record(tableIndicesSchema),
});

export type IndicesConfig = v.Infer<typeof indicesConfigSchema>;
