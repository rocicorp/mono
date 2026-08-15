import {nanoid} from 'nanoid';
import {sleep} from '../../../shared/src/sleep.ts';
import type {PostgresDB} from '../../src/types/pg.ts';
import type {LoadGeneratorFn, LoadStats} from './config.ts';

/**
 * Creates a load generator that generates realistic zbugs activity:
 * - Creates new issues (with title, description, creator, project)
 * - Updates existing issue modified timestamps / statuses
 * - Adds new comments to issues
 */
export function createZbugsLoadGenerator(
  options: {
    projectID?: string | undefined;
    creatorIDs?: readonly string[] | undefined;
    ratioNewIssues?: number | undefined;
    ratioComments?: number | undefined;
  } = {},
): LoadGeneratorFn {
  const projectID = options.projectID ?? 'proj_default';
  const creatorIDs =
    options.creatorIDs && options.creatorIDs.length > 0
      ? options.creatorIDs
      : ['usr_0000', 'usr_0001', 'usr_0002'];
  const ratioNewIssues = options.ratioNewIssues ?? 0.4;
  const ratioComments = options.ratioComments ?? 0.4;

  return async (
    db: PostgresDB,
    ratePerSec: number,
    durationSec: number,
    signal: AbortSignal,
  ): Promise<LoadStats> => {
    const knownIssueIDs: string[] = [];
    try {
      const existing = await db<{id: string}[]>`
        SELECT id FROM "issue" LIMIT 100
      `;
      for (const row of existing) {
        knownIssueIDs.push(row.id);
      }
    } catch {
      // Table might be freshly seeded or empty
    }

    let writesAttempted = 0;
    let writesSucceeded = 0;
    let writesFailed = 0;

    const startTime = performance.now();
    const endTime = startTime + durationSec * 1000;
    const intervalMs = ratePerSec > 0 ? 1000 / ratePerSec : 0;

    let nextTick = startTime;

    while (performance.now() < endTime && !signal.aborted) {
      writesAttempted++;
      const now = Date.now();
      const rand = Math.random();
      const creatorID =
        creatorIDs[Math.floor(Math.random() * creatorIDs.length)];

      try {
        if (rand < ratioNewIssues || knownIssueIDs.length === 0) {
          // 1. Create new issue
          const issueID = `iss_${nanoid(10)}`;
          await db`
            INSERT INTO "issue" (
              "id", "title", "open", "modified", "created",
              "projectID", "creatorID", "description", "visibility"
            ) VALUES (
              ${issueID},
              ${'Issue ' + writesAttempted + ': Benchmark load item'},
              true,
              ${now},
              ${now},
              ${projectID},
              ${creatorID},
              ${'Description for benchmark generated issue ' + issueID},
              ${'public'}
            )
          `;
          knownIssueIDs.push(issueID);
          if (knownIssueIDs.length > 500) {
            knownIssueIDs.shift();
          }
        } else if (rand < ratioNewIssues + ratioComments) {
          // 2. Add a comment to an existing issue
          const targetIssueID =
            knownIssueIDs[Math.floor(Math.random() * knownIssueIDs.length)];
          const commentID = `cmt_${nanoid(10)}`;
          await db.begin(async tx => {
            await tx`
              INSERT INTO "comment" (
                "id", "issueID", "created", "body", "creatorID"
              ) VALUES (
                ${commentID},
                ${targetIssueID},
                ${now},
                ${'Benchmark comment on ' + targetIssueID + ' at ' + new Date().toISOString()},
                ${creatorID}
              )
            `;
            await tx`
              UPDATE "issue"
              SET "modified" = ${now}
              WHERE "id" = ${targetIssueID}
            `;
          });
        } else {
          // 3. Update existing issue status
          const targetIssueID =
            knownIssueIDs[Math.floor(Math.random() * knownIssueIDs.length)];
          const toggleOpen = Math.random() > 0.5;
          await db`
            UPDATE "issue"
            SET "modified" = ${now}, "open" = ${toggleOpen}
            WHERE "id" = ${targetIssueID}
          `;
        }

        writesSucceeded++;
      } catch {
        writesFailed++;
      }

      if (intervalMs > 0) {
        nextTick += intervalMs;
        const delay = nextTick - performance.now();
        if (delay > 0) {
          await sleep(delay);
        }
      }
    }

    const durationMs = performance.now() - startTime;
    const actualRate =
      durationMs > 0 ? (writesSucceeded / durationMs) * 1000 : 0;

    return {
      writesAttempted,
      writesSucceeded,
      writesFailed,
      durationMs,
      actualRate,
    };
  };
}

/**
 * Generic load generator for inserting batches of rows into any table.
 */
export function createBatchInsertGenerator(options: {
  tableName: string;
  batchSize: number;
  rowFactory: (index: number) => Record<string, unknown>;
}): LoadGeneratorFn {
  const {tableName, batchSize, rowFactory} = options;

  return async (
    db: PostgresDB,
    ratePerSec: number,
    durationSec: number,
    signal: AbortSignal,
  ): Promise<LoadStats> => {
    let writesAttempted = 0;
    let writesSucceeded = 0;
    let writesFailed = 0;

    const startTime = performance.now();
    const endTime = startTime + durationSec * 1000;
    const intervalMs = ratePerSec > 0 ? (1000 * batchSize) / ratePerSec : 0;

    let nextTick = startTime;

    while (performance.now() < endTime && !signal.aborted) {
      const rows: Record<string, unknown>[] = [];
      for (let i = 0; i < batchSize; i++) {
        rows.push(rowFactory(writesAttempted + i));
      }
      writesAttempted += batchSize;

      try {
        await db`
          INSERT INTO ${db(tableName)} ${db(rows)}
        `;
        writesSucceeded += batchSize;
      } catch {
        writesFailed += batchSize;
      }

      if (intervalMs > 0) {
        nextTick += intervalMs;
        const delay = nextTick - performance.now();
        if (delay > 0) {
          await sleep(delay);
        }
      }
    }

    const durationMs = performance.now() - startTime;
    const actualRate =
      durationMs > 0 ? (writesSucceeded / durationMs) * 1000 : 0;

    return {
      writesAttempted,
      writesSucceeded,
      writesFailed,
      durationMs,
      actualRate,
    };
  };
}
