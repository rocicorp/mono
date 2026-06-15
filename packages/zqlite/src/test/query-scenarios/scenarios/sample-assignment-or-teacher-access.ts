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
const teacherAccessRelationship = relationshipName(
  educationAppRelationships.assignment,
  'teacher_access',
);

export default {
  name: 'sample assignment OR teacher access splits into selective roots',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const teacherAccess = tables.teacher_assignment_access;

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
    teacherAccessStmt.run(102, 1, 'class');
    teacherAccessStmt.run(103, 1, 'group');
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, exists, or}) =>
        or(
          cmp(colName(assignment, 'id'), '=', 4),
          exists(teacherAccessRelationship, access =>
            access.where(colName(teacherAccess, 'teacher_id'), '=', 1),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'or',
        conditions: [
          {},
          {
            type: 'correlatedSubquery',
            flip: true,
          },
        ],
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(
    //     id = sampleAssignmentId
    //     OR EXISTS teacher_assignment_access(teacher_id = currentTeacher)
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- keep the one sample row
    //     `-- probe teacher access once per assignment
    //
    // Optimized plan:
    //
    //   assignment(id = sampleAssignmentId) ----------------------.
    //                                                            +-- union
    //   teacher_assignment_access(teacher_id = currentTeacher) --'
    //     `-- fetch assignment by assignment_id
    //
    // Intuition:
    //
    //   Public or sample exceptions are common permission branches. They
    //   should not force the permission side back into a parent table scan.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
        calls: 4,
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
      },
    ],
    rows: [
      {id: 103, teacher_id: 2, archived_at: null, created_at: 103},
      {id: 102, teacher_id: 2, archived_at: null, created_at: 102},
      {id: 101, teacher_id: 2, archived_at: null, created_at: 101},
      {id: 4, teacher_id: 2, archived_at: null, created_at: 4},
    ],
  },
  // Safety note:
  //
  //   The fast plan wants to union assignment ids from two roots:
  //
  //     assignment(local branch) -------------------.
  //                                                 +-- assignment ids
  //     teacher_access(teacher = currentTeacher) --'
  //
  //   That is unsafe for a client system WHERE EXISTS because the access row
  //   is also synced as helper evidence. Returning only assignment ids would
  //   make the client row set and CAP row-set signature too small.
  knownFailure: {
    reason:
      'Root union is currently disabled for client system WHERE EXISTS branches because helper rows must remain attached to the synced query row set.',
    current:
      'The planner flips teacher access, but the builder keeps the generic OR plan so access helper rows still hydrate.',
    desired:
      'Split assignment and teacher access roots while preserving teacher access helper rows.',
    currentSQL: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?,?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
    engineIdea:
      'Teach root union to preserve relationship payloads and child row changes across branch handoffs.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
