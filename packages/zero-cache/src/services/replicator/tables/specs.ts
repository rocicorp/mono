export type ColumnSpec = {
  dataType: string;
  characterMaximumLength: number | null;
  columnDefault: string | null;
};

export type TableSpec = {
  schema: string;
  name: string;
  columns: Record<string, ColumnSpec>;
  primaryKey: string[];
};
