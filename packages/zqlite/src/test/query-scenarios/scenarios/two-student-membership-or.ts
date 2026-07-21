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
  name: 'OR across two membership branches merges and flips one child scan',
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
      .where(({exists, or}) =>
        or(
          exists(assignmentToStudentRelationship, q =>
            q.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
          exists(assignmentToStudentRelationship, q =>
            q.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-2',
            ),
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
              op: 'IN',
              left: {type: 'column', name: 'student_id'},
              right: {type: 'literal', value: ['student-1', 'student-2']},
            },
          },
        },
      },
    },
    planDebug: ['flipped'],
    // Submitted ZQL:
    //
    //   assignment.where(
    //     EXISTS assignment_to_student(student_id = 'student-1')
    //     OR EXISTS assignment_to_student(student_id = 'student-2')
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- probe membership for student-1
    //     `-- probe membership for student-2
    //
    // Optimized plan:
    //
    //   EXISTS membership(student_id = 'student-1')
    //      OR
    //   EXISTS membership(student_id = 'student-2')
    //              |
    //              v
    //   EXISTS membership(student_id IN ['student-1', 'student-2'])
    //
    //   assignment_to_student(student_id IN ['student-1', 'student-2'])
    //     `-- fetch assignment by assignment_id
    //
    // Intuition:
    //
    //   This is the right parent-id plan, but it is not row-set safe for
    //   client helper rows yet. Each original EXISTS branch can hydrate its
    //   own helper evidence. A merged branch would need to preserve that
    //   evidence before it can replace the two scans with one IN scan.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" IN (SELECT value FROM json_each(?)) ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
        calls: 3,
      },
    ],
  },
  // Safety note:
  //
  //   A normal database optimizer can merge these two semi-joins:
  //
  //     EXISTS membership(student = 1)
  //       OR
  //     EXISTS membership(student = 2)
  //
  //   into one membership(student IN [1, 2]) scan.
  //
  //   In Zero, the client-system helper rows are part of the synced row set:
  //
  //     assignment 101
  //       `-- membership helper row for student 1
  //
  //   The old unmerged shape can hydrate helper evidence per EXISTS branch.
  //   The merged shape must prove it will hydrate the same evidence before it
  //   is safe, especially around helper limits and CAP row-set signatures.
  knownFailure: {
    reason:
      'Same-relationship OR merge is disabled for client system WHERE EXISTS branches because merging can shrink helper evidence even when parent ids stay the same.',
    current:
      'The planner flips both membership branches separately, so each client helper branch can still hydrate its own evidence.',
    desired:
      'Use one membership IN scan while preserving exactly the helper rows each original EXISTS branch would have synced.',
    currentSQL: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 2,
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?) ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
    engineIdea:
      'Give OR merge a relationship-aware helper evidence model, or only merge client helpers after proving the helper cap and hydrated row set are unchanged.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
