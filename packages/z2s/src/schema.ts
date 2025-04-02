export type ServerTableSchema = {
  [columnName: string]: ServerColumnSchema;
};

export type ServerColumnSchema = {
  type: string;
  isEnum: boolean;
};

export type ServerSchema = {
  [tableName: string]: ServerTableSchema;
};
