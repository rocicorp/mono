import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {
  SchemaValue,
  TableSchema,
} from '../../../../zero-schema/src/table-schema.ts';
import type {Database} from '../../db.ts';

const assignment = table('assignment')
  .columns({
    id: number(),
    teacher_id: number(),
    archived_at: string().optional(),
    created_at: number(),
  })
  .primaryKey('id');

const assignmentToStudent = table('assignment_to_student')
  .columns({
    assignment_id: number(),
    student_id: string(),
    created_at: number(),
  })
  .primaryKey('assignment_id', 'student_id');

const assignmentRelationships = relationships(assignment, ({many}) => ({
  assignment_to_student: many({
    sourceField: ['id'],
    destField: ['assignment_id'],
    destSchema: assignmentToStudent,
  }),
}));

export const educationAppSchema = createSchema({
  tables: [assignment, assignmentToStudent],
  relationships: [assignmentRelationships],
});

export const educationAppTables = educationAppSchema.tables;
export const educationAppRelationships = educationAppSchema.relationships;

export function tableServerName<const TTable extends TableSchema>(
  tableSchema: TTable,
): TTable['serverName'] extends string ? TTable['serverName'] : TTable['name'] {
  return (tableSchema.serverName ??
    tableSchema.name) as TTable['serverName'] extends string
    ? TTable['serverName']
    : TTable['name'];
}

export function columnName<
  const TTable extends TableSchema,
  const TColumn extends keyof TTable['columns'] & string,
>(_tableSchema: TTable, column: TColumn): TColumn {
  return column;
}

export function columnServerName<
  const TTable extends TableSchema,
  const TColumn extends keyof TTable['columns'] & string,
>(
  tableSchema: TTable,
  column: TColumn,
): TTable['columns'][TColumn] extends {
  serverName: infer ServerName extends string;
}
  ? ServerName
  : TColumn {
  const schemaValue = tableSchema.columns[column] as SchemaValue;
  return (schemaValue.serverName ??
    column) as TTable['columns'][TColumn] extends {
    serverName: infer ServerName extends string;
  }
    ? ServerName
    : TColumn;
}

export function relationshipName<
  const TRelationships extends Record<string, unknown>,
  const TRelationship extends keyof TRelationships & string,
>(_relationshipsSchema: TRelationships, relationship: TRelationship) {
  return relationship;
}

export function createEducationAppTables(db: Database) {
  const assignment = educationAppTables.assignment;
  const assignmentToStudent = educationAppTables.assignment_to_student;

  db.exec(`
    CREATE TABLE ${tableServerName(assignment)} (
      ${columnServerName(assignment, 'id')} INTEGER PRIMARY KEY,
      ${columnServerName(assignment, 'teacher_id')} INTEGER,
      ${columnServerName(assignment, 'archived_at')} TEXT,
      ${columnServerName(assignment, 'created_at')} INTEGER
    );
    CREATE UNIQUE INDEX assignment_id_unique ON ${tableServerName(assignment)}(${columnServerName(assignment, 'id')});
    CREATE INDEX assignment_teacher_id_idx ON ${tableServerName(assignment)}(${columnServerName(assignment, 'teacher_id')});
    CREATE INDEX assignment_created_at_idx ON ${tableServerName(assignment)}(${columnServerName(assignment, 'created_at')});

    CREATE TABLE ${tableServerName(assignmentToStudent)} (
      ${columnServerName(assignmentToStudent, 'assignment_id')} INTEGER,
      ${columnServerName(assignmentToStudent, 'student_id')} TEXT,
      ${columnServerName(assignmentToStudent, 'created_at')} INTEGER,
      PRIMARY KEY (${columnServerName(assignmentToStudent, 'assignment_id')}, ${columnServerName(assignmentToStudent, 'student_id')})
    );
    CREATE INDEX assignment_to_student_student_idx ON ${tableServerName(assignmentToStudent)}(${columnServerName(assignmentToStudent, 'student_id')});
  `);

  return educationAppTables;
}
