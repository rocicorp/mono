export type ServerTableSchema = {
  [columnName: string]: ServerColumnSchema;
};

export type ServerColumnSchema = {
  type: string;
  isEnum: boolean;
  isArray: boolean;
  /**
   * The Postgres schema (namespace) that the column's underlying type lives in.
   *
   * For array columns this is the namespace of the *element* type, since that
   * is the type name recorded in {@link ServerColumnSchema.type}.
   *
   * This is needed so that generated SQL can schema-qualify casts to
   * user-defined types (enums, domains, ...) that live outside of `public`.
   * Built-in types in `pg_catalog` and types in `public` are left unqualified
   * so generated SQL stays compatible with the default `search_path`.
   *
   * Left `undefined` when the namespace is unknown (e.g. legacy callers that
   * construct a `ServerColumnSchema` by hand).
   */
  typeSchema?: string | undefined;
};

export type ServerSchema = {
  [tableName: string]: ServerTableSchema;
};
