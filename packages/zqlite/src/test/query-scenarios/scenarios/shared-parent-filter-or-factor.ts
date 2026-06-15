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
  name: 'shared parent filter across OR could factor before flipping child',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, i % 250 === 0 ? 'archived' : null, i);
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
      .where(({and, cmp, exists, or}) =>
        or(
          and(
            cmp(colName(assignment, 'archived_at'), 'IS', null),
            exists(assignmentToStudentRelationship, q =>
              q.where(
                colName(assignmentToStudent, 'student_id'),
                '=',
                'student-1',
              ),
            ),
          ),
          and(
            cmp(colName(assignment, 'archived_at'), 'IS', null),
            exists(assignmentToStudentRelationship, q =>
              q.where(
                colName(assignmentToStudent, 'student_id'),
                '=',
                'student-2',
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
          {
            type: 'simple',
            left: {type: 'column', name: 'archived_at'},
            op: 'IS',
            right: {type: 'literal', value: null},
          },
          {
            type: 'or',
            conditions: [
              {type: 'correlatedSubquery', flip: true},
              {type: 'correlatedSubquery', flip: true},
            ],
          },
        ],
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(
    //     (archived_at IS null
    //       AND EXISTS assignment_to_student(student_id = 'student-1'))
    //     OR
    //     (archived_at IS null
    //       AND EXISTS assignment_to_student(student_id = 'student-2'))
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- branch 1 checks archived_at, then probes student-1
    //     `-- branch 2 checks archived_at again, then probes student-2
    //
    // Optimized plan:
    //
    //   shared archived_at IS null
    //              |
    //              v
    //   archived_at IS null
    //     AND (
    //       EXISTS membership(student_id = 'student-1')
    //       OR EXISTS membership(student_id = 'student-2')
    //     )
    //
    //   membership(student_id = 'student-1') --.
    //                                           +-- union assignment ids
    //   membership(student_id = 'student-2') --'
    //                                           |
    //                                           v
    //                         fetch assignment(id, archived_at IS null)
    //
    // Intuition:
    //
    //   The duplicate parent filter is factored once. The two client helper
    //   branches stay separate so each branch can hydrate the same helper
    //   evidence the submitted query asked for.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 2,
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?) AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?) AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
