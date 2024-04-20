export type ColumnSpec = {
  readonly dataType: string;
  readonly characterMaximumLength: number | null;
  readonly columnDefault: string | null;
  readonly notNull: boolean;
};

export type TableSpec = {
  readonly schema: string;
  readonly name: string;
  readonly columns: Readonly<Record<string, ColumnSpec>>;
  readonly primaryKey: readonly string[];
};
