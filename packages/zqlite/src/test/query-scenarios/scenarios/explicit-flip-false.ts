import type {QueryScenario} from '../../query-scenario.ts';
import {
  educationAppSchema,
  createEducationAppTables,
} from '../education-app.ts';

export default {
  name: 'explicit flip false keeps membership branch unflipped',
  schema: educationAppSchema,
  seed: db => {
    createEducationAppTables(db);

    const assignmentStmt = db.prepare(
      'INSERT INTO assignment (id, teacher_id, archived_at, created_at) VALUES (?, ?, ?, ?)',
    );
    for (let i = 1; i <= 25; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const membershipStmt = db.prepare(
      'INSERT INTO assignment_to_student (assignment_id, student_id, created_at) VALUES (?, ?, ?)',
    );
    membershipStmt.run(3, 'student-1', 3);
  },
  query: builder =>
    builder.assignment
      .where('archived_at', 'IS', null)
      .whereExists(
        'assignment_to_student',
        q => q.where('student_id', '=', 'student-1'),
        {flip: false},
      )
      .orderBy('created_at', 'desc')
      .orderBy('id', 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'and',
        conditions: [
          {},
          {
            type: 'correlatedSubquery',
            flip: false,
          },
        ],
      },
    },
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "assignment_id" = ? AND "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
