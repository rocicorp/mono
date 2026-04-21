import type {QueryScenario} from '../../query-scenario.ts';
import {
  colName,
  createEducationAppTables,
  educationAppRelationships,
  educationAppSchema,
  educationAppTables,
  relationshipName,
  tableName,
} from '../education-app.ts';

const assignment = educationAppTables.assignment;
const assignmentToStudent = educationAppTables.assignment_to_student;
const assignmentToStudentRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_student',
);

export default {
  name: 'simple student membership exists auto flips to membership root',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i === 4 ? 1 : 2, null, i);
    }

    const membershipStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    );
    membershipStmt.run(101, 'student-1', 101);
    membershipStmt.run(102, 'student-1', 102);
    membershipStmt.run(103, 'student-1', 103);
  },
  query: builder =>
    builder[assignment.name]
      .where(colName(assignment, 'archived_at'), 'IS', null)
      .whereExists(assignmentToStudentRelationship, q =>
        q.where(colName(assignmentToStudent, 'student_id'), '=', 'student-1'),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
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
    planDebug: ['flipped'],
    // Submitted ZQL:
    //
    //   assignment
    //     .where(archived_at IS null)
    //     .whereExists(assignment_to_student, student_id = 'student-1')
    //
    // Naive plan:
    //
    //   assignment(archived_at IS null)
    //     `-- for each assignment, probe membership by assignment_id
    //
    // Optimized plan:
    //
    //   assignment_to_student(student_id = 'student-1')
    //     `-- fetch assignment by assignment_id
    //         `-- keep it only if archived_at IS null
    //
    // Intuition:
    //
    //   The student membership index is smaller than the live assignment set,
    //   so it is cheaper to start from the child row and look up its parent.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
        calls: 3,
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
