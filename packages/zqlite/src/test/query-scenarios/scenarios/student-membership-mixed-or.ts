import type {QueryScenario} from '../../query-scenario.ts';
import {
  educationAppSchema,
  createEducationAppTables,
} from '../education-app.ts';

export default {
  name: 'student membership mixed OR auto flips membership branch',
  schema: educationAppSchema,
  seed: db => {
    createEducationAppTables(db);

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
  expectations: {
    optimizedAST: {
      where: {
        type: 'and',
        conditions: [
          {},
          {
            type: 'or',
            conditions: [
              {},
              {
                type: 'correlatedSubquery',
                flip: true,
              },
            ],
          },
        ],
      },
    },
    planDebug: ['Best plan: Attempt 2', 'FO ⋈ assignment_to_student: flipped'],
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
