export type ServerTableSchema = {
  [columnName: string]: ServerColumnSchema;
};

export type ServerColumnSchema = {
  type: string;
  // Should be used when casting to this type, has type parameters
  // (like length for character, and precision, scale for numeric) missing
  // from type but needed for correctly casting.
  castType: string;
  isEnum: boolean;
};

export type ServerSchema = {
  [tableName: string]: ServerTableSchema;
};
