import * as v from '../../shared/src/valita.ts';

import {rowSchema} from './data.ts';

export const rowCountsByQuerySchema = v.record(v.number());
export type RowCountsByQuery = v.Infer<typeof rowCountsByQuerySchema>;

export const rowCountsBySourceSchema = v.record(rowCountsByQuerySchema);
export type RowCountsBySource = v.Infer<typeof rowCountsBySourceSchema>;

export const rowsByQuerySchema = v.record(v.array(rowSchema));
export type RowsByQuery = v.Infer<typeof rowsByQuerySchema>;

export const rowsBySourceSchema = v.record(rowsByQuerySchema);
export type RowsBySource = v.Infer<typeof rowsBySourceSchema>;

export const analyzeQueryResultSchema = v.object({
  warnings: v.array(v.string()),
  syncedRows: v.record(v.array(rowSchema)).optional(),
  syncedRowCount: v.number(),
  start: v.number(),
  /** @deprecated Use start + elapsed instead */
  end: v.number(),
  elapsed: v.number().optional(),
  afterPermissions: v.string().optional(),
  /** @deprecated Use readRowCountsByQuery */
  vendedRowCounts: rowCountsBySourceSchema.optional(),
  /** @deprecated Use readRows */
  vendedRows: rowsBySourceSchema.optional(),
  plans: v.record(v.array(v.string())).optional(),
  readRows: rowsBySourceSchema.optional(),
  readRowCountsByQuery: rowCountsBySourceSchema.optional(),
  readRowCount: v.number().optional(),
  dbScansByQuery: rowCountsBySourceSchema.optional(),
});

export type AnalyzeQueryResult = v.Infer<typeof analyzeQueryResultSchema>;
