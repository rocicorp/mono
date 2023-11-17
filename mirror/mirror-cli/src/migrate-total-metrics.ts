import {getFirestore} from 'firebase-admin/firestore';
import {
  METRICS_COLLECTION_ID,
  YearMetrics,
  metricsSchema,
  monthMetricsDataConverter,
  totalMetricsDataConverter,
  totalMetricsPath,
  type Month,
} from 'mirror-schema/src/metrics.js';
import * as v from 'shared/src/valita.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function migrateTotalMetricsOptions(yargs: CommonYargsArgv) {
  return yargs.option('step', {
    desc: 'Which step to perform.',
    type: 'number',
    choices: [1, 2, 3],
  });
}

type MigrateTotalMetricsHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateTotalMetricsOptions>
>;

const legacyTotalMetricsSchema = v.object({
  teamID: v.string(),
  appID: v.string().nullable(),

  total: metricsSchema,
  // Keyed by string, e.g. "2023"
  year: v.record(metricsSchema),
});

export async function migrateTotalMetricsHandler(
  yargs: MigrateTotalMetricsHandlerArgs,
) {
  const {step} = yargs;
  const firestore = getFirestore();

  // Step 1: Migrate the format of the `year` field without adding monthly totals.
  await firestore.runTransaction(async tx => {
    const metrics = await tx.get(
      firestore.collectionGroup(METRICS_COLLECTION_ID),
    );
    metrics.docs.forEach(doc => {
      const data = doc.data();
      if (data.year) {
        const total = v.parse(data, legacyTotalMetricsSchema, 'passthrough');
        const newTotal = {
          ...total,
          yearMonth: null,
          year: Object.fromEntries(
            Object.entries(total.year).map(
              ([year, total]) =>
                [year, {total, month: {}}] as [string, YearMetrics],
            ),
          ),
        };
        console.log(
          `Migrating year field of ${doc.ref.path}`,
          JSON.stringify(newTotal, null, ' '),
        );
        if (step === 1) {
          tx.set(doc.ref.withConverter(totalMetricsDataConverter), newTotal);
        }
      }
    });
    if (step !== 1) {
      console.log('Step 1 skipped.');
    }
  });

  // Step 2: Scan the monthly docs and add their totals to the corresponding total doc.
  await firestore.runTransaction(async tx => {
    const monthMetrics = await tx.get(
      firestore
        .collectionGroup(METRICS_COLLECTION_ID)
        .withConverter(monthMetricsDataConverter)
        .where('yearMonth', '!=', null),
    );
    monthMetrics.docs.forEach(doc => {
      const month = doc.data();
      const totalDoc = totalMetricsPath(month.teamID, month.appID);
      const year = Math.floor(month.yearMonth / 100).toString();
      const mm = (month.yearMonth % 100).toString() as Month;
      console.log(
        `Month corresponds to ${totalDoc}:year.${year}.month.${mm}`,
        month,
      );
      if (step === 2) {
        tx.update(
          firestore.doc(totalDoc).withConverter(totalMetricsDataConverter),
          `year.${year}.month.${mm}`,
          month.total,
        );
      }
    });
    if (step !== 2) {
      console.log('Step 2 skipped');
    }
  });

  // Step 3: Verify that the new total docs adhere to the current schema.
  if (step === 3) {
    const migrated = await firestore
      .collectionGroup(METRICS_COLLECTION_ID)
      .withConverter(totalMetricsDataConverter)
      .where('yearMonth', '==', null)
      .get();
    migrated.docs.forEach(doc => {
      console.log(`Converted total ${doc.ref.path}`, doc.data());
    });
  }
}
