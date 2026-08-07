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
  knownFailure: {
    reason:
      'Both OR branches repeat the same parent predicate. Factoring that predicate out would leave one same relationship child OR that can flip as a single membership scan.',
    current: `
OR
  AND
    archived_at IS null
    exists student-1
  AND
    archived_at IS null
    exists student-2

Plan shape today:

student-1 => assignment with archived_at
student-2 => assignment with archived_at
`,
    desired: `
AND
  archived_at IS null
  exists child where:
    student-1 OR student-2

Desired plan shape:

assignment_to_student student-1 OR student-2 => assignment archived_at IS null
`,
    currentSQL: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND ("archived_at" IS ? OR "archived_at" IS ?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
    engineIdea:
      'Run a small boolean algebra normalization before join enumeration. Factor common simple parent predicates out of OR branches, then run the same relationship OR merge pass on the remaining child predicates.',
  },
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
            type: 'correlatedSubquery',
            flip: true,
            related: {
              subquery: {
                where: {
                  type: 'or',
                },
              },
            },
          },
        ],
      },
    },
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE ("student_id" = ? OR "student_id" = ?) ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
