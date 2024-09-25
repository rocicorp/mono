export type ColumnSpec = {
  readonly pos: number;
  readonly dataType: string;
  readonly characterMaximumLength: number | null;
  readonly notNull: boolean;
};

export type TableSpec = {
  readonly schema: string;
  readonly name: string;
  readonly columns: Readonly<Record<string, ColumnSpec>>;
  readonly primaryKey: readonly string[];
};

export type FilteredTableSpec = TableSpec & {
  readonly publications: Readonly<Record<string, {rowFilter: string | null}>>;
};

export type IndexSpec = {
  readonly schemaName: string;
  readonly tableName: string;
  readonly name: string;
  readonly unique: boolean;
  readonly columns: readonly string[];
};

export type MutableIndexSpec = IndexSpec & {
  readonly columns: string[];
};
