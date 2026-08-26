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
    concurrency?: number | undefined;
    rowsPerTx?: number | undefined;
  } = {},
): LoadGeneratorFn {
  const projectID = options.projectID ?? 'proj_default';
  const creatorIDs =
    options.creatorIDs && options.creatorIDs.length > 0
      ? options.creatorIDs
      : ['usr_0000', 'usr_0001', 'usr_0002'];
  const ratioNewIssues = options.ratioNewIssues ?? 0.4;
  const ratioComments = options.ratioComments ?? 0.4;
  const rowsPerTx = Math.max(1, options.rowsPerTx ?? 1);

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

    const txRatePerSec = ratePerSec / rowsPerTx;
    const numWorkers =
      options.concurrency ??
      Math.max(1, Math.min(10, Math.ceil(txRatePerSec / 200)));
    const workerTxRate = txRatePerSec / numWorkers;

    const startTime = performance.now();
    const endTime = startTime + durationSec * 1000;

    const runWorker = async (workerId: number) => {
      let localAttempted = 0;
      let localSucceeded = 0;
      let localFailed = 0;
      const intervalMs = workerTxRate > 0 ? 1000 / workerTxRate : 0;
      let nextTick = performance.now();

      while (performance.now() < endTime && !signal.aborted) {
        localAttempted += rowsPerTx;
        const now = Date.now();

        try {
          if (rowsPerTx === 1) {
            const rand = Math.random();
            const creatorID =
              creatorIDs[Math.floor(Math.random() * creatorIDs.length)];

            if (rand < ratioNewIssues || knownIssueIDs.length === 0) {
              // 1. Create new issue
              const issueID = `iss_${nanoid(10)}`;
              await db`
                INSERT INTO "issue" (
                  "id", "title", "open", "modified", "created",
                  "projectID", "creatorID", "description", "visibility"
                ) VALUES (
                  ${issueID},
                  ${'Issue ' + workerId + '-' + localAttempted + ': Benchmark load item'},
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
          } else {
            // Multi-row transaction
            await db.begin(async tx => {
              const newIssueRows = [];
              for (let i = 0; i < rowsPerTx; i++) {
                const issueID = `iss_${nanoid(10)}`;
                const creatorID =
                  creatorIDs[Math.floor(Math.random() * creatorIDs.length)];
                newIssueRows.push({
                  id: issueID,
                  title: `Issue ${workerId}-${localAttempted}-${i}: Bulk item`,
                  open: true,
                  modified: now,
                  created: now,
                  projectID,
                  creatorID,
                  description: `Description for bulk issue ${issueID}`,
                  visibility: 'public',
                });
                knownIssueIDs.push(issueID);
              }
              if (knownIssueIDs.length > 1000) {
                knownIssueIDs.splice(0, knownIssueIDs.length - 500);
              }
              await tx`
                INSERT INTO "issue" ${tx(newIssueRows)}
              `;
            });
          }

          localSucceeded += rowsPerTx;
        } catch {
          localFailed += rowsPerTx;
        }

        if (intervalMs > 0) {
          nextTick += intervalMs;
          const delay = nextTick - performance.now();
          if (delay > 0) {
            await sleep(delay);
          }
        }
      }

      return {
        attempted: localAttempted,
        succeeded: localSucceeded,
        failed: localFailed,
      };
    };

    const workerResults = await Promise.all(
      Array.from({length: numWorkers}, (_, i) => runWorker(i)),
    );

    const totalAttempted = workerResults.reduce((s, r) => s + r.attempted, 0);
    const totalSucceeded = workerResults.reduce((s, r) => s + r.succeeded, 0);
    const totalFailed = workerResults.reduce((s, r) => s + r.failed, 0);

    const durationMs = performance.now() - startTime;
    const actualRate =
      durationMs > 0 ? (totalSucceeded / durationMs) * 1000 : 0;

    return {
      writesAttempted: totalAttempted,
      writesSucceeded: totalSucceeded,
      writesFailed: totalFailed,
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
