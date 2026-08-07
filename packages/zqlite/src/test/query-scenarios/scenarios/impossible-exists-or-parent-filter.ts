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
  name: 'OR with impossible exists keeps only parent predicate',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i % 100 === 0 ? 1 : 2, null, i);
    }
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, exists, or}) =>
        or(
          cmp(colName(assignment, 'teacher_id'), '=', 1),
          exists(assignmentToStudentRelationship, q =>
            q.where(colName(assignmentToStudent, 'student_id'), 'IN', []),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'simple',
        left: {type: 'column', name: 'teacher_id'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(
    //     teacher_id = 1
    //     OR EXISTS assignment_to_student(student_id IN [])
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- keep rows where teacher_id = 1
    //     `-- also ask membership for an empty student set
    //
    // Optimized plan:
    //
    //   EXISTS membership(student_id IN []) -> FALSE
    //
    //   teacher_id = 1 OR FALSE
    //              |
    //              v
    //         teacher_id = 1
    //
    // Intuition:
    //
    //   An empty IN list cannot find a child row, so the OR falls back to the
    //   only branch that can return assignments.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "teacher_id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
