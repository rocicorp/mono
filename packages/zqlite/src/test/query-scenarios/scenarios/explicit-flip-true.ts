import type {QueryScenario} from '../../query-scenario.ts';
import {
  educationAppSchema,
  createEducationAppTables,
} from '../education-app.ts';

export default {
  name: 'explicit flip true keeps membership branch flipped',
  schema: educationAppSchema,
  seed: db => {
    const {assignment, assignmentToStudent} = createEducationAppTables(db);

    const assignmentStmt = db.prepare(
      `INSERT INTO ${assignment.table} (${assignment.cols.id}, ${assignment.cols.teacher_id}, ${assignment.cols.archived_at}, ${assignment.cols.created_at}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 25; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const membershipStmt = db.prepare(
      `INSERT INTO ${assignmentToStudent.table} (${assignmentToStudent.cols.assignment_id}, ${assignmentToStudent.cols.student_id}, ${assignmentToStudent.cols.created_at}) VALUES (?, ?, ?)`,
    );
    membershipStmt.run(3, 'student-1', 3);
  },
  query: builder =>
    builder.assignment
      .where('archived_at', 'IS', null)
      .whereExists(
        'assignment_to_student',
        q => q.where('student_id', '=', 'student-1'),
        {flip: true},
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
            flip: true,
          },
        ],
      },
    },
    sql: [
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
