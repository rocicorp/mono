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
  name: 'mixed OR ignores false branch and auto flips membership branch',
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
      .where(({and, cmp, exists, or}) =>
        and(
          cmp(colName(assignment, 'archived_at'), 'IS', null),
          or(
            cmp(colName(assignment, 'teacher_id'), '=', 1),
            or(),
            exists(assignmentToStudentRelationship, q =>
              q.where(
                colName(assignmentToStudent, 'student_id'),
                '=',
                'student-1',
              ),
            ),
          ),
        ),
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
    planDebug: ['flipped'],
    // Submitted ZQL:
    //
    //   assignment.where(
    //     archived_at IS null
    //     AND (
    //       teacher_id = 1
    //       OR FALSE
    //       OR EXISTS assignment_to_student(student_id = 'student-1')
    //     )
    //   )
    //
    // Naive plan:
    //
    //   assignment(archived_at IS null)
    //     |-- check teacher_id = 1
    //     |-- carry a FALSE branch that can never match
    //     `-- if needed, probe membership by assignment_id
    //
    // Optimized plan:
    //
    //   teacher_id = 1 OR FALSE OR EXISTS membership(...)
    //              |
    //              v
    //   teacher_id = 1 OR EXISTS membership(...)
    //
    //   assignment(archived_at IS null, teacher_id = 1) ----------.
    //                                                             +-- union
    //   assignment_to_student(student_id = 'student-1') -> parent -'
    //       `-- keep parent only if archived_at IS null
    //
    // Intuition:
    //
    //   The FALSE branch disappears before costing, then the remaining OR gets
    //   the same split root treatment as the mixed parent and child case.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE ("archived_at" IS ? AND "teacher_id" = ?) ORDER BY "created_at" desc, "id" asc',
      },
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
  // Safety note:
  //
  //   Removing the FALSE branch is safe. Splitting the remaining OR is not
  //   safe yet because the membership EXISTS is client system evidence:
  //
  //     assignment row
  //       `-- membership helper row
  //
  //   The helper row has to hydrate and has to be counted by the CAP row-set
  //   signature.
  knownFailure: {
    reason:
      'Root union is currently disabled for client system WHERE EXISTS branches because those helper rows are part of the synced row set and CAP row-set signature.',
    current:
      'The impossible OR branch is simplified and the membership branch flips, but the builder keeps the generic OR plan to preserve helper row hydration.',
    desired:
      'Ignore the false branch, split the remaining parent and membership roots, and still sync membership helper rows.',
    currentSQL: [
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
        calls: 3,
      },
    ],
    engineIdea:
      'Make root union relationship aware so branch splitting does not drop client system helper rows or their signature entries.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
