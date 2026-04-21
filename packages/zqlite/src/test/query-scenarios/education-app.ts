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

const teacher = table('teacher')
  .columns({
    id: number(),
    user_id: string(),
    role: string(),
    school_id: number(),
  })
  .primaryKey('id');

const teacherAssignmentAccess = table('teacher_assignment_access')
  .columns({
    assignment_id: number(),
    teacher_id: number(),
    access_kind: string(),
  })
  .primaryKey('assignment_id', 'teacher_id');

const classroom = table('class')
  .columns({
    id: number(),
    status: string(),
    school_id: number(),
  })
  .primaryKey('id');

const assignmentToClass = table('assignment_to_class')
  .columns({
    assignment_id: number(),
    class_id: number(),
  })
  .primaryKey('assignment_id', 'class_id');

const classToStudent = table('class_to_student')
  .columns({
    class_id: number(),
    student_id: string(),
  })
  .primaryKey('class_id', 'student_id');

const studentGroup = table('student_group')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

const assignmentToGroup = table('assignment_to_group')
  .columns({
    assignment_id: number(),
    group_id: number(),
  })
  .primaryKey('assignment_id', 'group_id');

const groupToStudent = table('group_to_student')
  .columns({
    group_id: number(),
    student_id: string(),
  })
  .primaryKey('group_id', 'student_id');

const assignmentRelationships = relationships(assignment, ({many}) => ({
  assignment_to_student: many({
    sourceField: ['id'],
    destField: ['assignment_id'],
    destSchema: assignmentToStudent,
  }),
  teacher_access: many({
    sourceField: ['id'],
    destField: ['assignment_id'],
    destSchema: teacherAssignmentAccess,
  }),
  assignment_to_class: many({
    sourceField: ['id'],
    destField: ['assignment_id'],
    destSchema: assignmentToClass,
  }),
  assignment_to_group: many({
    sourceField: ['id'],
    destField: ['assignment_id'],
    destSchema: assignmentToGroup,
  }),
}));

const teacherAssignmentAccessRelationships = relationships(
  teacherAssignmentAccess,
  ({one}) => ({
    teacher: one({
      sourceField: ['teacher_id'],
      destField: ['id'],
      destSchema: teacher,
    }),
  }),
);

const assignmentToClassRelationships = relationships(
  assignmentToClass,
  ({one}) => ({
    class: one({
      sourceField: ['class_id'],
      destField: ['id'],
      destSchema: classroom,
    }),
  }),
);

const classRelationships = relationships(classroom, ({many}) => ({
  class_to_student: many({
    sourceField: ['id'],
    destField: ['class_id'],
    destSchema: classToStudent,
  }),
}));

const assignmentToGroupRelationships = relationships(
  assignmentToGroup,
  ({one}) => ({
    group: one({
      sourceField: ['group_id'],
      destField: ['id'],
      destSchema: studentGroup,
    }),
  }),
);

const groupRelationships = relationships(studentGroup, ({many}) => ({
  group_to_student: many({
    sourceField: ['id'],
    destField: ['group_id'],
    destSchema: groupToStudent,
  }),
}));

export const educationAppSchema = createSchema({
  tables: [
    assignment,
    assignmentToStudent,
    teacher,
    teacherAssignmentAccess,
    classroom,
    assignmentToClass,
    classToStudent,
    studentGroup,
    assignmentToGroup,
    groupToStudent,
  ],
  relationships: [
    assignmentRelationships,
    teacherAssignmentAccessRelationships,
    assignmentToClassRelationships,
    classRelationships,
    assignmentToGroupRelationships,
    groupRelationships,
  ],
});

export const educationAppTables = educationAppSchema.tables;
export const educationAppRelationships = educationAppSchema.relationships;

export function tableName<const TTable extends TableSchema>(
  tableSchema: TTable,
): TTable['serverName'] extends string ? TTable['serverName'] : TTable['name'] {
  return (tableSchema.serverName ??
    tableSchema.name) as TTable['serverName'] extends string
    ? TTable['serverName']
    : TTable['name'];
}

export function colName<
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
  const teacher = educationAppTables.teacher;
  const teacherAssignmentAccess = educationAppTables.teacher_assignment_access;
  const classroom = educationAppTables.class;
  const assignmentToClass = educationAppTables.assignment_to_class;
  const classToStudent = educationAppTables.class_to_student;
  const studentGroup = educationAppTables.student_group;
  const assignmentToGroup = educationAppTables.assignment_to_group;
  const groupToStudent = educationAppTables.group_to_student;

  db.exec(`
    CREATE TABLE ${tableName(assignment)} (
      ${colName(assignment, 'id')} INTEGER PRIMARY KEY,
      ${colName(assignment, 'teacher_id')} INTEGER,
      ${colName(assignment, 'archived_at')} TEXT,
      ${colName(assignment, 'created_at')} INTEGER
    );
    CREATE UNIQUE INDEX assignment_id_unique ON ${tableName(assignment)}(${colName(assignment, 'id')});
    CREATE INDEX assignment_teacher_id_idx ON ${tableName(assignment)}(${colName(assignment, 'teacher_id')});
    CREATE INDEX assignment_created_at_idx ON ${tableName(assignment)}(${colName(assignment, 'created_at')});

    CREATE TABLE ${tableName(assignmentToStudent)} (
      ${colName(assignmentToStudent, 'assignment_id')} INTEGER,
      ${colName(assignmentToStudent, 'student_id')} TEXT,
      ${colName(assignmentToStudent, 'created_at')} INTEGER,
      PRIMARY KEY (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')})
    );
    CREATE INDEX assignment_to_student_student_idx ON ${tableName(assignmentToStudent)}(${colName(assignmentToStudent, 'student_id')});

    CREATE TABLE ${tableName(teacher)} (
      ${colName(teacher, 'id')} INTEGER PRIMARY KEY,
      ${colName(teacher, 'user_id')} TEXT,
      ${colName(teacher, 'role')} TEXT,
      ${colName(teacher, 'school_id')} INTEGER
    );
    CREATE INDEX teacher_user_id_idx ON ${tableName(teacher)}(${colName(teacher, 'user_id')});

    CREATE TABLE ${tableName(teacherAssignmentAccess)} (
      ${colName(teacherAssignmentAccess, 'assignment_id')} INTEGER,
      ${colName(teacherAssignmentAccess, 'teacher_id')} INTEGER,
      ${colName(teacherAssignmentAccess, 'access_kind')} TEXT,
      PRIMARY KEY (${colName(teacherAssignmentAccess, 'assignment_id')}, ${colName(teacherAssignmentAccess, 'teacher_id')})
    );
    CREATE INDEX teacher_assignment_access_teacher_idx ON ${tableName(teacherAssignmentAccess)}(${colName(teacherAssignmentAccess, 'teacher_id')});
    CREATE INDEX teacher_assignment_access_assignment_idx ON ${tableName(teacherAssignmentAccess)}(${colName(teacherAssignmentAccess, 'assignment_id')});

    CREATE TABLE ${tableName(classroom)} (
      ${colName(classroom, 'id')} INTEGER PRIMARY KEY,
      ${colName(classroom, 'status')} TEXT,
      ${colName(classroom, 'school_id')} INTEGER
    );
    CREATE INDEX class_status_idx ON ${tableName(classroom)}(${colName(classroom, 'status')});

    CREATE TABLE ${tableName(assignmentToClass)} (
      ${colName(assignmentToClass, 'assignment_id')} INTEGER,
      ${colName(assignmentToClass, 'class_id')} INTEGER,
      PRIMARY KEY (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')})
    );
    CREATE INDEX assignment_to_class_class_idx ON ${tableName(assignmentToClass)}(${colName(assignmentToClass, 'class_id')});

    CREATE TABLE ${tableName(classToStudent)} (
      ${colName(classToStudent, 'class_id')} INTEGER,
      ${colName(classToStudent, 'student_id')} TEXT,
      PRIMARY KEY (${colName(classToStudent, 'class_id')}, ${colName(classToStudent, 'student_id')})
    );
    CREATE INDEX class_to_student_student_idx ON ${tableName(classToStudent)}(${colName(classToStudent, 'student_id')});

    CREATE TABLE ${tableName(studentGroup)} (
      ${colName(studentGroup, 'id')} INTEGER PRIMARY KEY,
      ${colName(studentGroup, 'name')} TEXT
    );

    CREATE TABLE ${tableName(assignmentToGroup)} (
      ${colName(assignmentToGroup, 'assignment_id')} INTEGER,
      ${colName(assignmentToGroup, 'group_id')} INTEGER,
      PRIMARY KEY (${colName(assignmentToGroup, 'assignment_id')}, ${colName(assignmentToGroup, 'group_id')})
    );
    CREATE INDEX assignment_to_group_group_idx ON ${tableName(assignmentToGroup)}(${colName(assignmentToGroup, 'group_id')});

    CREATE TABLE ${tableName(groupToStudent)} (
      ${colName(groupToStudent, 'group_id')} INTEGER,
      ${colName(groupToStudent, 'student_id')} TEXT,
      PRIMARY KEY (${colName(groupToStudent, 'group_id')}, ${colName(groupToStudent, 'student_id')})
    );
    CREATE INDEX group_to_student_student_idx ON ${tableName(groupToStudent)}(${colName(groupToStudent, 'student_id')});
  `);

  return educationAppTables;
}
