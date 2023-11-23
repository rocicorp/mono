import {getFirestore} from 'firebase-admin/firestore';
import {firestoreDataConverter} from 'mirror-schema/src/converter.js';
import {
  METRICS_COLLECTION_ID,
  metricsSchema,
  monthMetricsSchema,
  totalMetricsSchema,
  type Hour,
  type Metrics,
  type Month,
} from 'mirror-schema/src/metrics.js';
import * as v from 'shared/src/valita.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function migrateLeafMetricsOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print but do not execute actions',
    type: 'boolean',
    default: true,
  });
}

type MigrateLeafMetricsHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateLeafMetricsOptions>
>;

const legacyDayMetricsSchema = v.object({
  total: metricsSchema,
  hour: v.object({
    ['0']: metricsSchema.optional(),
    ['1']: metricsSchema.optional(),
    ['2']: metricsSchema.optional(),
    ['3']: metricsSchema.optional(),
    ['4']: metricsSchema.optional(),
    ['5']: metricsSchema.optional(),
    ['6']: metricsSchema.optional(),
    ['7']: metricsSchema.optional(),
    ['8']: metricsSchema.optional(),
    ['9']: metricsSchema.optional(),
    ['10']: metricsSchema.optional(),
    ['11']: metricsSchema.optional(),
    ['12']: metricsSchema.optional(),
    ['13']: metricsSchema.optional(),
    ['14']: metricsSchema.optional(),
    ['15']: metricsSchema.optional(),
    ['16']: metricsSchema.optional(),
    ['17']: metricsSchema.optional(),
    ['18']: metricsSchema.optional(),
    ['19']: metricsSchema.optional(),
    ['20']: metricsSchema.optional(),
    ['21']: metricsSchema.optional(),
    ['22']: metricsSchema.optional(),
    ['23']: metricsSchema.optional(),
  }),
});

export const legacyMonthMetricsSchema = v.object({
  teamID: v.string(),
  appID: v.string().nullable(), // null for Team-level metrics.
  yearMonth: v.number(),

  total: metricsSchema,
  day: v.object({
    ['1']: legacyDayMetricsSchema.optional(),
    ['2']: legacyDayMetricsSchema.optional(),
    ['3']: legacyDayMetricsSchema.optional(),
    ['4']: legacyDayMetricsSchema.optional(),
    ['5']: legacyDayMetricsSchema.optional(),
    ['6']: legacyDayMetricsSchema.optional(),
    ['7']: legacyDayMetricsSchema.optional(),
    ['8']: legacyDayMetricsSchema.optional(),
    ['9']: legacyDayMetricsSchema.optional(),
    ['10']: legacyDayMetricsSchema.optional(),
    ['11']: legacyDayMetricsSchema.optional(),
    ['12']: legacyDayMetricsSchema.optional(),
    ['13']: legacyDayMetricsSchema.optional(),
    ['14']: legacyDayMetricsSchema.optional(),
    ['15']: legacyDayMetricsSchema.optional(),
    ['16']: legacyDayMetricsSchema.optional(),
    ['17']: legacyDayMetricsSchema.optional(),
    ['18']: legacyDayMetricsSchema.optional(),
    ['19']: legacyDayMetricsSchema.optional(),
    ['20']: legacyDayMetricsSchema.optional(),
    ['21']: legacyDayMetricsSchema.optional(),
    ['22']: legacyDayMetricsSchema.optional(),
    ['23']: legacyDayMetricsSchema.optional(),
    ['24']: legacyDayMetricsSchema.optional(),
    ['25']: legacyDayMetricsSchema.optional(),
    ['26']: legacyDayMetricsSchema.optional(),
    ['27']: legacyDayMetricsSchema.optional(),
    ['28']: legacyDayMetricsSchema.optional(),
    ['29']: legacyDayMetricsSchema.optional(),
    ['30']: legacyDayMetricsSchema.optional(),
    ['31']: legacyDayMetricsSchema.optional(),
  }),
});

const legacyYearMetricsSchema = v.object({
  total: metricsSchema,
  month: v.object({
    ['1']: metricsSchema.optional(),
    ['2']: metricsSchema.optional(),
    ['3']: metricsSchema.optional(),
    ['4']: metricsSchema.optional(),
    ['5']: metricsSchema.optional(),
    ['6']: metricsSchema.optional(),
    ['7']: metricsSchema.optional(),
    ['8']: metricsSchema.optional(),
    ['9']: metricsSchema.optional(),
    ['10']: metricsSchema.optional(),
    ['11']: metricsSchema.optional(),
    ['12']: metricsSchema.optional(),
  }),
});

const legacyTotalMetricsSchema = v.object({
  teamID: v.string(),
  appID: v.string().nullable(),
  yearMonth: v.null(),
  total: metricsSchema,
  year: v.record(legacyYearMetricsSchema),
});

export async function migrateLeafMetricsHandler(
  yargs: MigrateLeafMetricsHandlerArgs,
) {
  const {dryRun} = yargs;
  const firestore = getFirestore();

  await firestore.runTransaction(async tx => {
    const [monthMetrics, totalMetrics] = await Promise.all([
      tx.get(
        firestore
          .collectionGroup(METRICS_COLLECTION_ID)
          .withConverter(firestoreDataConverter(legacyMonthMetricsSchema))
          .where('yearMonth', '!=', null),
      ),
      tx.get(
        firestore
          .collectionGroup(METRICS_COLLECTION_ID)
          .withConverter(firestoreDataConverter(legacyTotalMetricsSchema))
          .where('yearMonth', '==', null),
      ),
    ]);
    monthMetrics.docs.forEach(doc => {
      const month = doc.data();
      Object.entries(month.day).forEach(([_, day]) => {
        if (day) {
          Object.entries(day.hour).forEach(([hour, metrics]) => {
            day.hour[hour as Hour] = {total: metrics} as Metrics; // Appease the compiler. Runtime schema will be validated next.
          });
        }
      });
      // Check that the object now conforms to the new schema.
      v.assert(month, monthMetricsSchema);
      console.log(
        `Migrating ${doc.ref.path}`,
        JSON.stringify(month, null, ' '),
      );
      if (!dryRun) {
        tx.set(doc.ref, month);
      }
    });
    totalMetrics.docs.forEach(doc => {
      const total = doc.data();
      Object.entries(total.year).forEach(([_, year]) => {
        if (year) {
          Object.entries(year.month).forEach(([month, metrics]) => {
            year.month[month as Month] = {total: metrics} as Metrics; // Appease the compiler. Runtime schema will be validated next.
          });
        }
      });
      // Check that the object now conforms to the new schema.
      v.assert(total, totalMetricsSchema);
      console.log(
        `Migrating ${doc.ref.path}`,
        JSON.stringify(total, null, ' '),
      );
      if (!dryRun) {
        tx.set(doc.ref, total);
      }
    });
  });
}
