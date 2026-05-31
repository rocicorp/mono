import * as v from '../../../../../shared/src/valita.ts';

// PG-specific messages passed in the table metadata and backfill ID
// messages.

export const columnMetadataSchema = v.object({
  attNum: v.number(),
});

export type ColumnMetadata = v.Infer<typeof columnMetadataSchema>;

export const tableMetadataSchema = v.object({
  schemaOID: v.number(),
  relationOID: v.number(),
  rowKey: v.record(columnMetadataSchema),
});

export type TableMetadata = v.Infer<typeof tableMetadataSchema>;
