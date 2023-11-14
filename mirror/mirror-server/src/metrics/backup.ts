import type {Bucket} from '@google-cloud/storage';
import type {Analytics} from 'cloudflare-api/src/analytics.js';
import {logger} from 'firebase-functions';
import * as v from 'shared/src/valita.js';

// Backs up the previous week's worth of metrics to the specified bucket in
// the file `{cloudflare-account-id}/{table-name}/YYYY-MM-DD~YYYY-MM-DD`
export async function backupWeekBefore(
  date: Date,
  analytics: Analytics,
  table: string,
  bucket: Bucket,
): Promise<void> {
  if (date.getUTCDay() === 0 && date.getUTCHours() < 6) {
    // Sanity check: Don't allow this to run between Sunday midnight to 6AM.
    // This ensures a 6 hour buffer for all of the data to arrive in WAE (which
    // purports to make data available in minutes).
    throw new Error(
      `Need more time to ensure that WAE data has been processed.`,
    );
  }
  const first = new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() - date.getUTCDay() - 7,
    ),
  );
  const last = new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() - date.getUTCDay(),
    ),
  );

  const results: string[] = [];
  let totalCount = 0;
  logger.log(`Start date: ${first.toISOString()}`);
  for (let i = 0, start = first; i < 7; i++) {
    const end = new Date(
      Date.UTC(
        start.getUTCFullYear(),
        start.getUTCMonth(),
        start.getUTCDate() + 1,
      ),
    );
    const query = `SELECT * FROM ${table} WHERE timestamp >= toDateTime(${
      start.getTime() / 1000
    }) AND timestamp < toDateTime(${
      end.getTime() / 1000
    }) ORDER BY timestamp FORMAT JSONEachRow`;
    const result = await analytics.queryRaw(query);
    const count = checkResults(result);
    logger.info(`Num results: ${count}`);
    results.push(result);
    totalCount += count;
    start = end;
  }

  const firstDate = first.toISOString().split('T')[0];
  const lastDate = last.toISOString().split('T')[0];
  const {accountID} = analytics;
  const file = bucket.file(`${accountID}/${table}/${firstDate}~${lastDate}`);
  logger.info(`Saving ${totalCount} rows to ${file.name}`);
  await file.save(results.join(''), {
    resumable: false,
    gzip: true,
    contentType: 'text/plain',
  });
}

export function checkResults(str: string) {
  for (let pos = -1, count = 0; ; count++) {
    pos = str.indexOf('\n', pos + 1);
    if (pos < 0) {
      return count;
    }
    if (count === 0) {
      // Parse the first row to ensure that it is a well-formed response
      // and not an error.
      const firstRow = str.substring(0, pos);
      const row = v.parse(JSON.parse(firstRow), rawRowSchema, 'passthrough');
      logger.info(`Validated first row`, row);
    }
  }
}

const rawRowSchema = v.object({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _sample_interval: v.number(),
  blob1: v.string(),
  blob2: v.string(),
  blob3: v.string(),
  blob4: v.string(),
  blob5: v.string(),
  blob6: v.string(),
  blob7: v.string(),
  blob8: v.string(),
  blob9: v.string(),
  blob10: v.string(),
  blob11: v.string(),
  blob12: v.string(),
  blob13: v.string(),
  blob14: v.string(),
  blob15: v.string(),
  blob16: v.string(),
  blob17: v.string(),
  blob18: v.string(),
  blob19: v.string(),
  blob20: v.string(),
  double1: v.number(),
  double2: v.number(),
  double3: v.number(),
  double4: v.number(),
  double5: v.number(),
  double6: v.number(),
  double7: v.number(),
  double8: v.number(),
  double9: v.number(),
  double10: v.number(),
  double11: v.number(),
  double12: v.number(),
  double13: v.number(),
  double14: v.number(),
  double15: v.number(),
  double16: v.number(),
  double17: v.number(),
  double18: v.number(),
  double19: v.number(),
  double20: v.number(),
  timestamp: v.string(),
  index1: v.string(),
  dataset: v.string(),
});
