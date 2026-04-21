import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {TableSchema} from '../../../../zero-schema/src/table-schema.ts';
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

type TableReference<TTable extends TableSchema> = {
  readonly table: TTable['serverName'] extends string
    ? TTable['serverName']
    : TTable['name'];
  readonly cols: {
    readonly [K in keyof TTable['columns'] &
      string]: TTable['columns'][K] extends {
      serverName: infer ServerName extends string;
    }
      ? ServerName
      : K;
  };
};

function tableReference<const TTable extends TableSchema>(
  tableSchema: TTable,
): TableReference<TTable> {
  return {
    table: tableSchema.serverName ?? tableSchema.name,
    cols: Object.fromEntries(
      Object.entries(tableSchema.columns).map(([name, column]) => [
        name,
        column.serverName ?? name,
      ]),
    ),
  } as TableReference<TTable>;
}

const educationAppTables = {
  assignment: tableReference(educationAppSchema.tables.assignment),
  assignmentToStudent: tableReference(
    educationAppSchema.tables.assignment_to_student,
  ),
} as const;

export function createEducationAppTables(db: Database) {
  const {assignment, assignmentToStudent} = educationAppTables;

  db.exec(`
    CREATE TABLE ${assignment.table} (
      ${assignment.cols.id} INTEGER PRIMARY KEY,
      ${assignment.cols.teacher_id} INTEGER,
      ${assignment.cols.archived_at} TEXT,
      ${assignment.cols.created_at} INTEGER
    );
    CREATE UNIQUE INDEX assignment_id_unique ON ${assignment.table}(${assignment.cols.id});
    CREATE INDEX assignment_teacher_id_idx ON ${assignment.table}(${assignment.cols.teacher_id});
    CREATE INDEX assignment_created_at_idx ON ${assignment.table}(${assignment.cols.created_at});

    CREATE TABLE ${assignmentToStudent.table} (
      ${assignmentToStudent.cols.assignment_id} INTEGER,
      ${assignmentToStudent.cols.student_id} TEXT,
      ${assignmentToStudent.cols.created_at} INTEGER,
      PRIMARY KEY (${assignmentToStudent.cols.assignment_id}, ${assignmentToStudent.cols.student_id})
    );
    CREATE INDEX assignment_to_student_student_idx ON ${assignmentToStudent.table}(${assignmentToStudent.cols.student_id});
  `);

  return educationAppTables;
}
