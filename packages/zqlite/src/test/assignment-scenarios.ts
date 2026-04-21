import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {QueryScenario} from './query-scenario.ts';

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

const schema = createSchema({
  tables: [assignment, assignmentToStudent],
  relationships: [assignmentRelationships],
});

export const studentMembershipMixedORScenario = {
  schema,
  setup: db => {
    db.exec(`
      CREATE TABLE assignment (
        id INTEGER PRIMARY KEY,
        teacher_id INTEGER,
        archived_at TEXT,
        created_at INTEGER
      );
      CREATE UNIQUE INDEX assignment_id_unique ON assignment(id);
      CREATE INDEX assignment_teacher_id_idx ON assignment(teacher_id);
      CREATE INDEX assignment_created_at_idx ON assignment(created_at);

      CREATE TABLE assignment_to_student (
        assignment_id INTEGER,
        student_id TEXT,
        created_at INTEGER,
        PRIMARY KEY (assignment_id, student_id)
      );
      CREATE INDEX assignment_to_student_student_idx ON assignment_to_student(student_id);
    `);

    const assignmentStmt = db.prepare(
      'INSERT INTO assignment (id, teacher_id, archived_at, created_at) VALUES (?, ?, ?, ?)',
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i === 4 ? 1 : 2, null, i);
    }

    const membershipStmt = db.prepare(
      'INSERT INTO assignment_to_student (assignment_id, student_id, created_at) VALUES (?, ?, ?)',
    );
    membershipStmt.run(101, 'student-1', 101);
    membershipStmt.run(102, 'student-1', 102);
    membershipStmt.run(103, 'student-1', 103);
  },
  query: builder =>
    builder.assignment
      .where(({and, cmp, exists, or}) =>
        and(
          cmp('archived_at', 'IS', null),
          or(
            cmp('teacher_id', '=', 1),
            exists('assignment_to_student', q =>
              q.where('student_id', '=', 'student-1'),
            ),
          ),
        ),
      )
      .orderBy('created_at', 'desc')
      .orderBy('id', 'asc'),
} satisfies QueryScenario<typeof schema>;
