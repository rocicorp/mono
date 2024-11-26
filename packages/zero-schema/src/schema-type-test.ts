import type {
  Relationship,
  SchemaValue,
  SourceOrTableSchema,
} from './table-schema.js';
import type * as v from '../../shared/src/valita.js';
import type {
  relationshipSchema,
  schemaSchema,
  schemaValueSchema,
  sourceOrTableSchemaSchema,
} from './schema-config.js';
import type {Schema} from './schema.js';

type MakeAllFieldsRequired<T> = {
  [K in keyof T]-?: MakeAllFieldsRequired<T[K]>;
};

(
  t: SchemaValue,
  inferredT: v.Infer<typeof schemaValueSchema>,
  tR: MakeAllFieldsRequired<SchemaValue>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof schemaValueSchema>>,
) => {
  t satisfies v.Infer<typeof schemaValueSchema>;
  inferredT satisfies SchemaValue;

  inferredTR satisfies MakeAllFieldsRequired<SchemaValue>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof schemaValueSchema>>;
};

(
  t: SourceOrTableSchema,
  inferredT: v.Infer<typeof sourceOrTableSchemaSchema>,
  tR: MakeAllFieldsRequired<SourceOrTableSchema>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof sourceOrTableSchemaSchema>>,
) => {
  t satisfies v.Infer<typeof sourceOrTableSchemaSchema>;
  inferredT satisfies SourceOrTableSchema;

  inferredTR satisfies MakeAllFieldsRequired<SourceOrTableSchema>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof sourceOrTableSchemaSchema>>;
};

(
  t: Schema,
  inferredT: v.Infer<typeof schemaSchema>,
  tR: MakeAllFieldsRequired<Schema>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof schemaSchema>>,
) => {
  t satisfies v.Infer<typeof schemaSchema>;
  inferredT satisfies Schema;

  inferredTR satisfies MakeAllFieldsRequired<Schema>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof schemaSchema>>;
};

// v.Infer<typeof relationshipSchema> should be assignable to Relationship but not vice versa because
// relationshipSchema does not allow type Lazy<T> = T | (() => T); only Lazy<T> = T
(
  //t: Relationship,
  inferredT: v.Infer<typeof relationshipSchema>,
  //tR: MakeAllFieldsRequired<Relationship>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof relationshipSchema>>,
) => {
  //t satisfies v.Infer<typeof relationshipSchema>;
  inferredT satisfies Relationship;
  inferredTR satisfies MakeAllFieldsRequired<Relationship>;
  //tR satisfies MakeAllFieldsRequired<v.Infer<typeof relationshipSchema>>;
};
