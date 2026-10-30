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
   * user-defined types (enums, extension types, ...), including ones in
   * `public` whose name matches a built-in type. Domains are reported as their
   * base type, so for a domain column this is the namespace of the type that
   * the domain wraps. (An array of a domain is not itself a domain, so its
   * element type is the domain.) Built-in types in `pg_catalog` are left
   * unqualified.
   *
   * Left `undefined` when the namespace is unknown (e.g. legacy callers that
   * construct a `ServerColumnSchema` by hand).
   */
  typeSchema?: string | undefined;
};

export type ServerSchema = {
  [tableName: string]: ServerTableSchema;
};
