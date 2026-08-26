import {nanoid} from 'nanoid';
import {h128} from '../../../shared/src/hash.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-permissions/src/permissions.ts';
import type {PostgresDB} from '../../src/types/pg.ts';
import {schema} from './default-queries.ts';

export interface SeedOptions {
  appID?: string | undefined;
  numUsers?: number | undefined;
  numProjects?: number | undefined;
  numIssues?: number | undefined;
  commentsPerIssue?: number | undefined;
}

/**
 * Initializes the PostgreSQL schema for zbugs, creates permissions,
 * publication, and seeds initial users, projects, labels, and issues.
 */
export async function seedZbugsDatabase(
  db: PostgresDB,
  opts: SeedOptions = {},
): Promise<void> {
  const appID = opts.appID ?? 'zero';
  const numUsers = opts.numUsers ?? 10;
  const numProjects = opts.numProjects ?? 5;
  const numIssues = opts.numIssues ?? 200;
  const commentsPerIssue = opts.commentsPerIssue ?? 2;

  // 1. Create tables
  await db.unsafe(`
    CREATE TABLE IF NOT EXISTS "user" (
      "id" TEXT PRIMARY KEY,
      "login" TEXT NOT NULL UNIQUE,
      "name" TEXT,
      "avatar" TEXT NOT NULL,
      "role" TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS "project" (
      "id" TEXT PRIMARY KEY,
      "name" TEXT NOT NULL UNIQUE,
      "lowerCaseName" TEXT NOT NULL UNIQUE,
      "issueCountEstimate" INT,
      "supportsSearch" BOOL NOT NULL DEFAULT false,
      "markURL" TEXT,
      "logoURL" TEXT
    );

    CREATE TABLE IF NOT EXISTS "issue" (
      "id" TEXT PRIMARY KEY,
      "shortID" INT,
      "title" TEXT NOT NULL,
      "open" BOOL NOT NULL DEFAULT true,
      "modified" INT8 NOT NULL,
      "created" INT8 NOT NULL,
      "projectID" TEXT NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
      "creatorID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE,
      "assigneeID" TEXT REFERENCES "user"("id") ON DELETE SET NULL,
      "description" TEXT NOT NULL,
      "visibility" TEXT NOT NULL DEFAULT 'public'
    );

    CREATE TABLE IF NOT EXISTS "viewState" (
      "userID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE,
      "issueID" TEXT NOT NULL REFERENCES "issue"("id") ON DELETE CASCADE,
      "viewed" INT8 NOT NULL,
      PRIMARY KEY ("userID", "issueID")
    );

    CREATE TABLE IF NOT EXISTS "comment" (
      "id" TEXT PRIMARY KEY,
      "issueID" TEXT NOT NULL REFERENCES "issue"("id") ON DELETE CASCADE,
      "created" INT8 NOT NULL,
      "body" TEXT NOT NULL,
      "creatorID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS "label" (
      "id" TEXT PRIMARY KEY,
      "name" TEXT NOT NULL,
      "projectID" TEXT NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
      UNIQUE ("projectID", "name")
    );

    CREATE TABLE IF NOT EXISTS "issueLabel" (
      "issueID" TEXT NOT NULL REFERENCES "issue"("id") ON DELETE CASCADE,
      "labelID" TEXT NOT NULL REFERENCES "label"("id") ON DELETE CASCADE,
      "projectID" TEXT NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
      PRIMARY KEY ("labelID", "issueID")
    );

    CREATE TABLE IF NOT EXISTS "emoji" (
      "id" TEXT PRIMARY KEY,
      "value" TEXT NOT NULL,
      "annotation" TEXT NOT NULL,
      "subjectID" TEXT NOT NULL,
      "creatorID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE,
      "created" INT8 NOT NULL
    );

    CREATE TABLE IF NOT EXISTS "userPref" (
      "key" TEXT NOT NULL,
      "userID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE,
      "value" TEXT NOT NULL,
      PRIMARY KEY ("userID", "key")
    );

    CREATE TABLE IF NOT EXISTS "issueNotifications" (
      "userID" TEXT NOT NULL REFERENCES "user"("id") ON DELETE CASCADE,
      "issueID" TEXT NOT NULL REFERENCES "issue"("id") ON DELETE CASCADE,
      "subscribed" BOOL NOT NULL DEFAULT true,
      "created" INT8 NOT NULL,
      PRIMARY KEY ("userID", "issueID")
    );

    CREATE INDEX IF NOT EXISTS "issue_projectID_idx" ON "issue"("projectID");
    CREATE INDEX IF NOT EXISTS "issue_creatorID_idx" ON "issue"("creatorID");
    CREATE INDEX IF NOT EXISTS "issue_modified_idx" ON "issue"("modified" DESC);
    CREATE INDEX IF NOT EXISTS "comment_issueID_idx" ON "comment"("issueID");

    DROP PUBLICATION IF EXISTS zero_all;
    CREATE PUBLICATION zero_all FOR TABLE "user", "project", "issue", "comment", "label", "issueLabel", "viewState", "emoji", "userPref", "issueNotifications";
  `);

  // 2. Deploy open permissions for the appID
  const permissions = await definePermissions(schema, () => ({
    user: ANYONE_CAN_DO_ANYTHING,
    project: ANYONE_CAN_DO_ANYTHING,
    issue: ANYONE_CAN_DO_ANYTHING,
    comment: ANYONE_CAN_DO_ANYTHING,
    label: ANYONE_CAN_DO_ANYTHING,
    issueLabel: ANYONE_CAN_DO_ANYTHING,
    viewState: ANYONE_CAN_DO_ANYTHING,
    emoji: ANYONE_CAN_DO_ANYTHING,
    userPref: ANYONE_CAN_DO_ANYTHING,
    issueNotifications: ANYONE_CAN_DO_ANYTHING,
  }));

  const permsJSON = JSON.stringify(permissions);
  const hash = h128(permsJSON).toString(16);

  await db.unsafe(`
    CREATE SCHEMA IF NOT EXISTS "${appID}";
    CREATE TABLE IF NOT EXISTS "${appID}".permissions (
      permissions JSON,
      hash TEXT
    );
    DELETE FROM "${appID}".permissions;
    INSERT INTO "${appID}".permissions (permissions, hash) VALUES ('${permsJSON}', '${hash}');
  `);

  // 3. Seed data
  const now = Date.now();

  // Users
  const userRows = [];
  for (let i = 0; i < numUsers; i++) {
    const id = `usr_${String(i).padStart(4, '0')}`;
    userRows.push({
      id,
      login: `user${i}`,
      name: `User ${i}`,
      avatar: `https://avatar.example.com/${i}.png`,
      role: i === 0 ? 'crew' : 'user',
    });
  }
  await db`
    INSERT INTO "user" ${db(userRows)}
    ON CONFLICT ("id") DO NOTHING
  `;

  // Projects
  const projectRows = [
    {
      id: 'proj_default',
      name: 'Zero',
      lowerCaseName: 'zero',
      issueCountEstimate: numIssues,
      supportsSearch: true,
      markURL: '',
      logoURL: '',
    },
  ];
  for (let i = 1; i < numProjects; i++) {
    projectRows.push({
      id: `proj_${i}`,
      name: `Project ${i}`,
      lowerCaseName: `project ${i}`,
      issueCountEstimate: 50,
      supportsSearch: true,
      markURL: '',
      logoURL: '',
    });
  }
  await db`
    INSERT INTO "project" ${db(projectRows)}
    ON CONFLICT ("id") DO NOTHING
  `;

  // Labels
  const labelNames = ['bug', 'feature', 'performance', 'docs', 'infra', 'ui'];
  const labelRows = [];
  for (const proj of projectRows) {
    for (const name of labelNames) {
      labelRows.push({
        id: `lbl_${proj.id}_${name}`,
        name,
        projectID: proj.id,
      });
    }
  }
  await db`
    INSERT INTO "label" ${db(labelRows)}
    ON CONFLICT ("id") DO NOTHING
  `;

  // Issues & Comments
  const issueRows = [];
  const commentRows = [];
  const issueLabelRows = [];

  for (let i = 0; i < numIssues; i++) {
    const issueID = `iss_${String(i).padStart(6, '0')}`;
    const proj = projectRows[i % projectRows.length];
    const creator = userRows[i % userRows.length];
    const assignee = userRows[(i + 1) % userRows.length];
    const created = now - (numIssues - i) * 60_000;
    const modified = created;

    issueRows.push({
      id: issueID,
      shortID: i + 1,
      title: `Benchmark Issue #${i + 1}: ${nanoid(8)}`,
      open: i % 5 !== 0,
      modified,
      created,
      projectID: proj.id,
      creatorID: creator.id,
      assigneeID: assignee.id,
      description: `Initial description for benchmark issue ${issueID}`,
      visibility: 'public',
    });

    const label = labelRows[i % labelRows.length];
    if (label.projectID === proj.id) {
      issueLabelRows.push({
        issueID,
        labelID: label.id,
        projectID: proj.id,
      });
    }

    for (let c = 0; c < commentsPerIssue; c++) {
      const commentID = `cmt_${issueID}_${c}`;
      commentRows.push({
        id: commentID,
        issueID,
        created: created + (c + 1) * 5000,
        body: `Comment ${c + 1} on ${issueID}`,
        creatorID: userRows[(i + c) % userRows.length].id,
      });
    }
  }

  const BATCH_SIZE = 500;
  for (let i = 0; i < issueRows.length; i += BATCH_SIZE) {
    const batch = issueRows.slice(i, i + BATCH_SIZE);
    await db`INSERT INTO "issue" ${db(batch)} ON CONFLICT ("id") DO NOTHING`;
  }

  if (issueLabelRows.length > 0) {
    for (let i = 0; i < issueLabelRows.length; i += BATCH_SIZE) {
      const batch = issueLabelRows.slice(i, i + BATCH_SIZE);
      await db`INSERT INTO "issueLabel" ${db(batch)} ON CONFLICT ("labelID", "issueID") DO NOTHING`;
    }
  }

  if (commentRows.length > 0) {
    for (let i = 0; i < commentRows.length; i += BATCH_SIZE) {
      const batch = commentRows.slice(i, i + BATCH_SIZE);
      await db`INSERT INTO "comment" ${db(batch)} ON CONFLICT ("id") DO NOTHING`;
    }
  }
}
