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
  name: 'child IN plus exclusion narrows before auto flipping',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const membershipStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    );
    membershipStmt.run(101, 'student-1', 101);
    membershipStmt.run(102, 'student-1', 102);
    membershipStmt.run(1_500, 'student-2', 1_500);
  },
  query: builder =>
    builder[assignment.name]
      .whereExists(assignmentToStudentRelationship, q =>
        q.where(({and, cmp}) =>
          and(
            cmp(colName(assignmentToStudent, 'student_id'), 'IN', [
              'student-1',
              'student-2',
            ]),
            cmp(colName(assignmentToStudent, 'student_id'), '!=', 'student-2'),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'correlatedSubquery',
        flip: true,
        related: {
          subquery: {
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'student_id'},
              right: {type: 'literal', value: 'student-1'},
            },
          },
        },
      },
    },
    planDebug: ['flipped'],
    // Submitted ZQL:
    //
    //   assignment.whereExists(
    //     assignment_to_student,
    //     student_id IN ['student-1', 'student-2']
    //       AND student_id != 'student-2'
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     `-- for each assignment, probe membership by assignment_id
    //         and evaluate both student predicates
    //
    // Optimized plan:
    //
    //   student_id IN ['student-1', 'student-2']
    //     AND student_id != 'student-2'
    //              |
    //              v
    //        student_id = 'student-1'
    //
    //   assignment_to_student(student_id = 'student-1')
    //     `-- fetch assignment by assignment_id
    //
    // Intuition:
    //
    //   The child filter simplifies to one student before planning, so the
    //   planner starts from the small membership index instead of walking every
    //   assignment.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
        calls: 2,
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
