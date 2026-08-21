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
const teacherAccess = educationAppTables.teacher_assignment_access;
const assignmentToClass = educationAppTables.assignment_to_class;
const teacherAccessRelationship = relationshipName(
  educationAppRelationships.assignment,
  'teacher_access',
);
const assignmentToClassRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_class',
);

export default {
  name: 'skewed cross relationship declines broad child intersection',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const teacherAccess = tables.teacher_assignment_access;
    const assignmentToClass = tables.assignment_to_class;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const teacherAccessStmt = db.prepare(
      `INSERT INTO ${tableName(teacherAccess)} (${colName(teacherAccess, 'assignment_id')}, ${colName(teacherAccess, 'teacher_id')}, ${colName(teacherAccess, 'access_kind')}) VALUES (?, ?, ?)`,
    );
    teacherAccessStmt.run(101, 1, 'direct');
    teacherAccessStmt.run(102, 1, 'direct');
    teacherAccessStmt.run(103, 1, 'direct');

    const assignmentToClassStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    );
    for (let i = 1; i <= 1_900; i++) {
      assignmentToClassStmt.run(i, 10);
    }
  },
  query: builder =>
    builder[assignment.name]
      .whereExists(teacherAccessRelationship, access =>
        access.where(colName(teacherAccess, 'teacher_id'), '=', 1),
      )
      .whereExists(assignmentToClassRelationship, assignmentClass =>
        assignmentClass.where(colName(assignmentToClass, 'class_id'), '=', 10),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'and',
        conditions: [
          {
            type: 'correlatedSubquery',
            flip: true,
          },
          {
            type: 'correlatedSubquery',
          },
        ],
      },
    },
    // Submitted ZQL:
    //
    //   assignment
    //     .whereExists(teacher_assignment_access, teacher_id = currentTeacher)
    //     .whereExists(assignment_to_class, class_id = veryLargeClass)
    //
    // Naive intersection:
    //
    //   teacher_assignment_access(teacher_id = currentTeacher) --.
    //                                                            +-- intersect assignment_id
    //   assignment_to_class(class_id = veryLargeClass) ----------'
    //
    // Optimized plan:
    //
    //   teacher_assignment_access(teacher_id = currentTeacher)
    //     `-- fetch assignment
    //           `-- probe class membership by assignment_id
    //
    // Intuition:
    //
    //   Intersections are only a win when sibling child scans are in the same
    //   ballpark. Here the teacher access branch has 3 rows, while the class
    //   branch has 1,900 rows. The guard keeps the tiny child root and probes
    //   the broad sibling by assignment id instead of materializing it.
    sql: [
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?,?) AND TRUE ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "assignment_id" = ? AND "class_id" = ?',
        calls: 3,
      },
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "assignment_id" = ? AND "class_id" = ? AND "class_id" = ?',
        calls: 3,
      },
    ],
    rows: [
      {id: 103, teacher_id: 2, archived_at: null, created_at: 103},
      {id: 102, teacher_id: 2, archived_at: null, created_at: 102},
      {id: 101, teacher_id: 2, archived_at: null, created_at: 101},
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
