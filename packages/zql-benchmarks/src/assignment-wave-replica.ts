/**
 * A replica shaped like the educator assignment-detail page the zero-cache
 * planning diagnosis replayed: 16 assignments, 136 students, 24 problems and
 * 973 problem trackers, reached through the same permission chain
 * (assignment -> teacher access, group -> teacher -> school -> school group,
 * class membership and co-teacher grants).
 *
 * Only the tables and columns the wave's transformed ASTs actually reference
 * are present, and every identity is synthetic (`*_emu_*`).
 */
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../zero-cache/src/services/replicator/schema/table-metadata.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';

const ASSIGNMENTS = 16;
const STUDENTS = 136;
const TRACKERS = 973;
const CLASSES = 6;
const GROUPS = 4;
const TEACHERS = 8;

const TABLES: {name: string; columns: string; primaryKey: PrimaryKey}[] = [
  {
    name: 'school_group',
    columns: 'id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'school',
    columns: 'id TEXT, group_id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'user',
    columns: 'id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'teacher',
    columns: 'id TEXT, user_id TEXT, school_id TEXT, role TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'student',
    columns: 'id TEXT, user_id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'class',
    columns: 'id TEXT, status TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'group',
    columns: 'id TEXT, teacher_id TEXT, name TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'assignment_group',
    columns: 'id TEXT, created_by_teacher_id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'assignment',
    columns: 'id TEXT, assignment_group_id TEXT, creation_reason TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'problem_tracker',
    columns: 'id TEXT, assignment_id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'conversation',
    columns: 'id TEXT, problem_tracker_id TEXT, created_at INTEGER',
    primaryKey: ['id'],
  },
  {
    name: 'mastery_assessment',
    columns: 'id TEXT, problem_tracker_id TEXT',
    primaryKey: ['id'],
  },
  {
    name: 'teacher_assignment_access',
    columns: 'assignment_id TEXT, teacher_id TEXT',
    primaryKey: ['assignment_id', 'teacher_id'],
  },
  {
    name: 'assignment_to_student',
    columns: 'assignment_id TEXT, student_id TEXT',
    primaryKey: ['assignment_id', 'student_id'],
  },
  {
    name: 'assignment_to_class',
    columns: 'assignment_id TEXT, class_id TEXT',
    primaryKey: ['assignment_id', 'class_id'],
  },
  {
    name: 'assignment_to_group',
    columns: 'assignment_id TEXT, group_id TEXT',
    primaryKey: ['assignment_id', 'group_id'],
  },
  {
    name: 'student_class_membership',
    columns: 'student_id TEXT, class_id TEXT',
    primaryKey: ['student_id', 'class_id'],
  },
  {
    name: 'teacher_class_access',
    columns: 'class_id TEXT, teacher_id TEXT',
    primaryKey: ['class_id', 'teacher_id'],
  },
  {
    name: 'teacher_to_co_teacher',
    columns: 'teacher_id TEXT, co_teacher_id TEXT',
    primaryKey: ['teacher_id', 'co_teacher_id'],
  },
  {
    name: 'group_to_student',
    columns: 'group_id TEXT, student_id TEXT',
    primaryKey: ['group_id', 'student_id'],
  },
];

/** The assignment `ROSTER_AST` and `TRACKERS_AST` are pinned to. */
export const ASSIGNMENT_ID = 'assignment_emu_lag_136';

/** The teacher user `ROSTER_AST` and `TRACKERS_AST` are authorized as. */
export const TEACHER_USER_ID = 'user_emu_lag_teacher';

export const primaryKeys = new Map<string, PrimaryKey>(
  TABLES.map(({name, primaryKey}) => [name, primaryKey]),
);

export function createAssignmentWaveReplica(): {
  db: Database;
  tableSpecs: Map<string, LiteAndZqlSpec>;
} {
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(CREATE_TABLE_METADATA_TABLE);

  for (const {name, columns, primaryKey} of TABLES) {
    db.exec(
      `CREATE TABLE "${name}" (${columns}, _0_version TEXT NOT NULL, ` +
        `PRIMARY KEY (${primaryKey.map(k => `"${k}"`).join(', ')}))`,
    );
  }

  const insert = (table: string, rows: Record<string, unknown>[]) => {
    if (rows.length === 0) {
      return;
    }
    const cols = Object.keys(rows[0]);
    const stmt = db.prepare(
      `INSERT INTO "${table}" (${cols.map(c => `"${c}"`).join(',')}, _0_version) ` +
        `VALUES (${cols.map(() => '?').join(',')}, '00')`,
    );
    for (const row of rows) {
      stmt.run(cols.map(c => row[c]));
    }
  };

  const range = <T>(n: number, f: (i: number) => T) =>
    Array.from({length: n}, (_, i) => f(i));

  insert('school_group', [{id: 'school_group_emu_1'}]);
  insert('school', [{id: 'school_emu_1', group_id: 'school_group_emu_1'}]);
  insert('user', [
    {id: TEACHER_USER_ID},
    ...range(STUDENTS, i => ({id: `user_emu_student_${i}`})),
  ]);
  insert(
    'teacher',
    range(TEACHERS, i => ({
      id: `teacher_emu_${i}`,
      user_id: i === 0 ? TEACHER_USER_ID : `user_emu_teacher_${i}`,
      school_id: 'school_emu_1',
      role: i === 0 ? 'owner' : 'member',
    })),
  );
  insert(
    'student',
    range(STUDENTS, i => ({
      id: `student_emu_${i}`,
      user_id: `user_emu_student_${i}`,
    })),
  );
  insert(
    'class',
    range(CLASSES, i => ({id: `class_emu_${i}`, status: 'active'})),
  );
  insert(
    'group',
    range(GROUPS, i => ({
      id: `group_emu_${i}`,
      teacher_id: `teacher_emu_${i % TEACHERS}`,
      name: `Group ${i}`,
    })),
  );
  insert(
    'assignment_group',
    range(ASSIGNMENTS, i => ({
      id: `assignment_group_emu_${i}`,
      created_by_teacher_id: `teacher_emu_${i % TEACHERS}`,
    })),
  );
  insert(
    'assignment',
    range(ASSIGNMENTS, i => ({
      id: i === 0 ? ASSIGNMENT_ID : `assignment_emu_${i}`,
      assignment_group_id: `assignment_group_emu_${i}`,
      creation_reason: 'teacher_created',
    })),
  );
  insert(
    'problem_tracker',
    range(TRACKERS, i => ({
      id: `problem_tracker_emu_${i}`,
      assignment_id:
        i % ASSIGNMENTS === 0
          ? ASSIGNMENT_ID
          : `assignment_emu_${i % ASSIGNMENTS}`,
    })),
  );
  insert(
    'conversation',
    range(TRACKERS, i => ({
      id: `conversation_emu_${i}`,
      problem_tracker_id: `problem_tracker_emu_${i}`,
      created_at: 1_700_000_000_000 + i,
    })),
  );
  insert(
    'mastery_assessment',
    range(TRACKERS / 2, i => ({
      id: `mastery_assessment_emu_${i}`,
      problem_tracker_id: `problem_tracker_emu_${i * 2}`,
    })),
  );
  insert(
    'teacher_assignment_access',
    range(ASSIGNMENTS, i => ({
      assignment_id: i === 0 ? ASSIGNMENT_ID : `assignment_emu_${i}`,
      teacher_id: `teacher_emu_${i % TEACHERS}`,
    })),
  );
  insert(
    'assignment_to_student',
    range(STUDENTS, i => ({
      assignment_id: ASSIGNMENT_ID,
      student_id: `student_emu_${i}`,
    })),
  );
  insert(
    'assignment_to_class',
    range(CLASSES, i => ({
      assignment_id: ASSIGNMENT_ID,
      class_id: `class_emu_${i}`,
    })),
  );
  insert(
    'assignment_to_group',
    range(GROUPS, i => ({
      assignment_id: ASSIGNMENT_ID,
      group_id: `group_emu_${i}`,
    })),
  );
  insert(
    'student_class_membership',
    range(STUDENTS, i => ({
      student_id: `student_emu_${i}`,
      class_id: `class_emu_${i % CLASSES}`,
    })),
  );
  insert(
    'teacher_class_access',
    range(CLASSES, i => ({
      class_id: `class_emu_${i}`,
      teacher_id: `teacher_emu_${i % TEACHERS}`,
    })),
  );
  insert(
    'teacher_to_co_teacher',
    range(TEACHERS - 1, i => ({
      teacher_id: 'teacher_emu_0',
      co_teacher_id: `teacher_emu_${i + 1}`,
    })),
  );
  insert(
    'group_to_student',
    range(STUDENTS, i => ({
      group_id: `group_emu_${i % GROUPS}`,
      student_id: `student_emu_${i}`,
    })),
  );

  db.exec('ANALYZE');

  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);
  return {db, tableSpecs};
}

/** A fresh {@link TableSource} per table, as a PipelineDriver builds. */
export function createAssignmentWaveSources(
  db: Database,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
): Record<string, TableSource> {
  const lc = createSilentLogContext();
  const sources: Record<string, TableSource> = {};
  for (const {name, primaryKey} of TABLES) {
    sources[name] = new TableSource(
      lc,
      testLogConfig,
      db,
      name,
      must(tableSpecs.get(name), `No spec for ${name}`).zqlSpec,
      primaryKey,
    );
  }
  return sources;
}
