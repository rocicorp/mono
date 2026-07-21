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
  name: 'permission access and class filter could intersect child keys',
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
      assignmentStmt.run(i, 2, i === 103 ? 'archived' : null, i);
    }

    const teacherAccessStmt = db.prepare(
      `INSERT INTO ${tableName(teacherAccess)} (${colName(teacherAccess, 'assignment_id')}, ${colName(teacherAccess, 'teacher_id')}, ${colName(teacherAccess, 'access_kind')}) VALUES (?, ?, ?)`,
    );
    teacherAccessStmt.run(101, 1, 'direct');
    teacherAccessStmt.run(102, 1, 'class');
    teacherAccessStmt.run(103, 1, 'group');
    teacherAccessStmt.run(104, 1, 'direct');

    const assignmentToClassStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    );
    assignmentToClassStmt.run(102, 10);
    assignmentToClassStmt.run(103, 10);
    assignmentToClassStmt.run(1_500, 10);
  },
  query: builder =>
    builder[assignment.name]
      .where(colName(assignment, 'archived_at'), 'IS', null)
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
          {},
          {
            type: 'correlatedSubquery',
          },
          {
            type: 'correlatedSubquery',
            flip: true,
          },
        ],
      },
    },
    // Submitted ZQL:
    //
    //   assignment
    //     .where(archived_at IS null)
    //     .whereExists(teacher_assignment_access, teacher_id = currentTeacher)
    //     .whereExists(assignment_to_class, class_id = selectedClass)
    //
    // Naive plan:
    //
    //   assignment(archived_at IS null)
    //     |-- probe teacher access by assignment_id
    //     `-- probe class membership by assignment_id
    //
    // Desired plan:
    //
    //   teacher_assignment_access(teacher_id = currentTeacher) --.
    //                                                            +-- intersect assignment_id
    //   assignment_to_class(class_id = selectedClass) ----------'
    //     `-- fetch assignment by assignment_id and archived_at
    //
    // Intuition:
    //
    //   This mirrors list screens that layer a permission table with a UI
    //   filter. Both child tables are selective, so the engine intersects
    //   assignment ids from the two child tables before loading parents.
    sql: [
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
        calls: 2,
      },
    ],
    rows: [{id: 102, teacher_id: 2, archived_at: null, created_at: 102}],
  },
  // Safety note:
  //
  //   This scenario looks like a permission shape, but in this harness both
  //   WHERE EXISTS branches are client system helpers. That means both child
  //   rows are synced evidence, not just private server checks:
  //
  //     assignment 102
  //       |-- teacher_access helper row
  //       `-- assignment_to_class helper row
  //
  //   A pure id intersection would find assignment 102 correctly while losing
  //   one of those helper rows.
  knownFailure: {
    reason:
      'Sibling intersection is currently disabled for client system WHERE EXISTS branches because helper rows must remain part of the synced row set.',
    current:
      'The planner flips the class branch, then probes teacher access per candidate assignment to preserve helper hydration.',
    desired:
      'Intersect teacher access and class membership child scans while preserving both helper row streams.',
    currentSQL: [
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?,?) AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "assignment_id" = ? AND "teacher_id" = ?',
        calls: 2,
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "assignment_id" = ? AND "teacher_id" = ? AND "teacher_id" = ?',
      },
    ],
    engineIdea:
      'Split server only permission checks from client helper relationships, or make intersection preserve all helper relationship evidence.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
