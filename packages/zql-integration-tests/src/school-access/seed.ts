/**
 * Generates a deterministic dataset that mirrors the customer's profile.
 *
 * The shape is deliberately built so that THREE of the four OR branches in
 * the normalized access query stay live at runtime, each producing matches
 * for every (student, class) membership. This is what stresses the OR
 * fan-out: every membership row gets evaluated against three independent
 * flipped-exists pipelines.
 *
 *   - 1 district, 1 school
 *   - 32 teachers at the school (31 regular + 1 school-administrator).
 *     The 1 admin is the requesting user.
 *   - 29 classes
 *   - teacher_to_class:
 *       - 29 rows for the 29 regular teachers (1:1 teacher → class)
 *       - 29 rows for the admin (admin teaches every class) → DIRECT branch
 *         resolves to admin and matches every class.
 *   - teacher_to_co_teacher:
 *       - 29 rows: each of the 29 teaching teachers grants admin co-teacher
 *         access → CO_TEACHER branch resolves to admin and matches every
 *         class via the granter's teacher_to_class.
 *   - school-admin role is held by admin → SCHOOL_ADMIN branch matches.
 *   - DISTRICT_ADMIN: dead. The scalar (user_id=USER, role='administrator')
 *     resolves to no row, so the branch is compiled out — admin holds the
 *     school-administrator role, not administrator. Matches the customer's
 *     reported plan, where one of the role-scoped branches dies.
 *   - `numStudents` students, `numStudents * membershipsPerStudent` total
 *     student_class_membership rows.
 *   - 1 teacher_student_access row per membership for the denorm shape.
 */

export const USER_ID = 'user-admin-1';

export const BASE_SEED = {
  baseNumClasses: 29,
  // numTeachers is derived: numClasses + 2 non-teaching + 1 admin
  baseNumStudents: 564,
  membershipsPerStudent: 2,
  adminTeacherId: 9999,
  districtId: 1,
  schoolId: 1,
} as const;

export type Seed = {
  numTeachers: number;
  numClasses: number;
  numStudents: number;
  membershipsPerStudent: number;
  adminTeacherId: number;
  districtId: number;
  schoolId: number;
};

export type ScaleOptions = {
  /** Multiplier on numStudents (and proportional memberships/access). */
  studentScale?: number;
  /**
   * Multiplier on numClasses and numTeachers (school topology). The
   * normalized OR branches do work proportional to school topology, not to
   * student data, so this is the axis that should grow the norm/denorm
   * ratio if our hypothesis is right.
   */
  topologyScale?: number;
};

export function makeSeed(opts: number | ScaleOptions = 1): Seed {
  const studentScale =
    typeof opts === 'number' ? opts : (opts.studentScale ?? 1);
  const topologyScale =
    typeof opts === 'number' ? 1 : (opts.topologyScale ?? 1);

  const numClasses = BASE_SEED.baseNumClasses * topologyScale;
  // numTeachers = numClasses (each teaches 1 class) + 2 non-teaching + 1 admin
  const numTeachers = numClasses + 3;

  return {
    numTeachers,
    numClasses,
    numStudents: BASE_SEED.baseNumStudents * studentScale,
    membershipsPerStudent: BASE_SEED.membershipsPerStudent,
    adminTeacherId: BASE_SEED.adminTeacherId,
    districtId: BASE_SEED.districtId,
    schoolId: BASE_SEED.schoolId,
  };
}

// Backwards-compatible default (scale = 1 on both axes) used by the original test.
export const SEED: Seed = makeSeed(1);

export const DDL = /* sql */ `
CREATE TABLE district (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE school (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  district_id INTEGER NOT NULL
);
CREATE INDEX school_district_id_idx ON school (district_id);

CREATE TABLE teacher (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  role TEXT NOT NULL,
  school_id INTEGER NOT NULL
);
CREATE UNIQUE INDEX teacher_user_id_unique ON teacher (user_id);
CREATE INDEX teacher_school_id_idx ON teacher (school_id);

CREATE TABLE teacher_to_co_teacher (
  id INTEGER PRIMARY KEY,
  from_teacher_id INTEGER NOT NULL,
  to_teacher_id INTEGER NOT NULL
);
CREATE INDEX teacher_to_co_teacher_from_idx ON teacher_to_co_teacher (from_teacher_id);
CREATE INDEX teacher_to_co_teacher_to_idx ON teacher_to_co_teacher (to_teacher_id);

CREATE TABLE class (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  school_id INTEGER NOT NULL
);
CREATE INDEX class_school_id_idx ON class (school_id);

CREATE TABLE teacher_to_class (
  id INTEGER PRIMARY KEY,
  teacher_id INTEGER NOT NULL,
  class_id INTEGER NOT NULL
);
CREATE INDEX teacher_to_class_teacher_id_idx ON teacher_to_class (teacher_id);
CREATE INDEX teacher_to_class_class_id_idx ON teacher_to_class (class_id);

CREATE TABLE student (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE student_class_membership (
  id INTEGER PRIMARY KEY,
  student_id INTEGER NOT NULL,
  class_id INTEGER NOT NULL
);
CREATE INDEX student_class_membership_student_id_idx ON student_class_membership (student_id);
CREATE INDEX student_class_membership_class_id_idx ON student_class_membership (class_id);

CREATE TABLE teacher_student_access (
  id INTEGER PRIMARY KEY,
  teacher_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL
);
CREATE INDEX teacher_student_access_teacher_id_idx ON teacher_student_access (teacher_id);
CREATE INDEX teacher_student_access_student_id_idx ON teacher_student_access (student_id);

CREATE TABLE teacher_class_access (
  id INTEGER PRIMARY KEY,
  teacher_id INTEGER NOT NULL,
  class_id INTEGER NOT NULL
);
CREATE INDEX teacher_class_access_teacher_id_idx ON teacher_class_access (teacher_id);
CREATE INDEX teacher_class_access_class_id_idx ON teacher_class_access (class_id);
`;

export function generateInserts(seed: Seed = SEED): string {
  const lines: string[] = [];
  const {
    numTeachers,
    numClasses,
    numStudents,
    membershipsPerStudent,
    adminTeacherId,
    districtId,
    schoolId,
  } = seed;

  lines.push(`INSERT INTO district VALUES (${districtId}, 'District 1');`);
  lines.push(
    `INSERT INTO school VALUES (${schoolId}, 'School 1', ${districtId});`,
  );

  // 31 regular teachers
  const regularTeachers = numTeachers - 1;
  const teacherValues: string[] = [];
  for (let i = 1; i <= regularTeachers; i++) {
    teacherValues.push(`(${i}, 'teacher-user-${i}', 'teacher', ${schoolId})`);
  }
  // 1 admin (the requester)
  teacherValues.push(
    `(${adminTeacherId}, '${USER_ID}', 'school-administrator', ${schoolId})`,
  );
  lines.push(
    `INSERT INTO teacher (id, user_id, role, school_id) VALUES ${teacherValues.join(
      ',\n  ',
    )};`,
  );

  // 29 classes
  const classValues: string[] = [];
  for (let i = 1; i <= numClasses; i++) {
    classValues.push(`(${i}, 'Class ${i}', ${schoolId})`);
  }
  lines.push(
    `INSERT INTO class (id, name, school_id) VALUES ${classValues.join(
      ',\n  ',
    )};`,
  );

  // teacher_to_class:
  //   - 1 row per regular teacher i in [1..numClasses]
  //   - 1 row per admin per class (admin teaches every class) — keeps the
  //     DIRECT branch live at runtime.
  const tcValues: string[] = [];
  let tcId = 1;
  for (let i = 1; i <= numClasses; i++) {
    tcValues.push(`(${tcId++}, ${i}, ${i})`);
  }
  for (let i = 1; i <= numClasses; i++) {
    tcValues.push(`(${tcId++}, ${adminTeacherId}, ${i})`);
  }
  lines.push(
    `INSERT INTO teacher_to_class (id, teacher_id, class_id) VALUES ${tcValues.join(
      ',\n  ',
    )};`,
  );

  // teacher_to_co_teacher: each of the 29 teaching regular teachers grants
  // admin co-teacher access. Keeps the CO_TEACHER branch live at runtime —
  // it walks back from admin to all granting teachers and from each granter
  // to their classes.
  const cotValues: string[] = [];
  for (let i = 1; i <= numClasses; i++) {
    cotValues.push(`(${i}, ${i}, ${adminTeacherId})`);
  }
  lines.push(
    `INSERT INTO teacher_to_co_teacher (id, from_teacher_id, to_teacher_id) VALUES ${cotValues.join(
      ',\n  ',
    )};`,
  );

  // students
  const studentValues: string[] = [];
  for (let s = 1; s <= numStudents; s++) {
    studentValues.push(`(${s}, 'Student ${s}')`);
  }
  lines.push(
    `INSERT INTO student (id, name) VALUES ${studentValues.join(',\n  ')};`,
  );

  // memberships: each student in `membershipsPerStudent` classes
  const membershipValues: string[] = [];
  let membershipId = 1;
  for (let s = 1; s <= numStudents; s++) {
    for (let m = 0; m < membershipsPerStudent; m++) {
      // Deterministic spread across classes
      const classId = ((s + m * 7) % numClasses) + 1;
      membershipValues.push(`(${membershipId++}, ${s}, ${classId})`);
    }
  }
  lines.push(
    `INSERT INTO student_class_membership (id, student_id, class_id) VALUES ${membershipValues.join(
      ',\n  ',
    )};`,
  );

  // teacher_student_access for the admin: 1 row per membership.
  // Admin sees every student-class pair at the school.
  const accessValues: string[] = [];
  let accessId = 1;
  for (let s = 1; s <= numStudents; s++) {
    for (let m = 0; m < membershipsPerStudent; m++) {
      accessValues.push(`(${accessId++}, ${adminTeacherId}, ${s})`);
    }
  }
  lines.push(
    `INSERT INTO teacher_student_access (id, teacher_id, student_id) VALUES ${accessValues.join(
      ',\n  ',
    )};`,
  );

  // teacher_class_access (smaller denorm): 1 row per (teacher_with_access,
  // class). For our admin, that's 1 row per class. Plus 1 row per regular
  // teaching teacher × the class they teach (direct access). The customer's
  // real maintenance trigger would also write rows for co-teacher grants
  // (granter's classes propagate to grantees) but for our query the admin
  // already has access via school-admin so we don't need separate rows.
  const tcaValues: string[] = [];
  let tcaId = 1;
  // Admin's class access: every class.
  for (let i = 1; i <= numClasses; i++) {
    tcaValues.push(`(${tcaId++}, ${adminTeacherId}, ${i})`);
  }
  // Each regular teacher's class access (direct): 1 row per class they teach.
  for (let i = 1; i <= numClasses; i++) {
    tcaValues.push(`(${tcaId++}, ${i}, ${i})`);
  }
  lines.push(
    `INSERT INTO teacher_class_access (id, teacher_id, class_id) VALUES ${tcaValues.join(
      ',\n  ',
    )};`,
  );

  return lines.join('\n');
}

export function generatePgContent(seed: Seed = SEED): string {
  return `${DDL}\n${generateInserts(seed)}\n`;
}
