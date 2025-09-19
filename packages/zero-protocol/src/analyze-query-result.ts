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
  end: v.number(),
  afterPermissions: v.string().optional(),
  vendedRowCounts: rowCountsBySourceSchema.optional(),
  vendedRows: rowsBySourceSchema.optional(),
  plans: v.record(v.array(v.string())).optional(),
});

export type AnalyzeQueryResult = v.Infer<typeof analyzeQueryResultSchema>;
