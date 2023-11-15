import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {appPath} from './deployment.js';
import * as path from './path.js';
import {teamPath} from './team.js';

/** Connection seconds reported incrementally from within the RoomDO. */
export const CONNECTION_SECONDS = 'cs';
/** Seconds that RoomDO's had connections. `cs / rs` is the average number of connections. */
export const ROOM_SECONDS = 'rs';
/** Connection seconds computed from completed FetchEvents. */
export const CONNECTION_LIFETIMES = 'cl';

export const metricSchema = v.union(
  v.literal(CONNECTION_SECONDS),
  v.literal(CONNECTION_LIFETIMES),
  v.literal(ROOM_SECONDS),
);

export type Metric = v.Infer<typeof metricSchema>;

// Data is stored in sparse objects, with short-named fields set
// when information is available, and 0-valued metrics implied
// for absent fields.
export const metricsSchema = v.object({
  [CONNECTION_SECONDS]: v.number().optional(),
  [CONNECTION_LIFETIMES]: v.number().optional(),
  [ROOM_SECONDS]: v.number().optional(),
});

export type Metrics = v.Infer<typeof metricsSchema>;

export type Hour =
  | '0'
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | '10'
  | '11'
  | '12'
  | '13'
  | '14'
  | '15'
  | '16'
  | '17'
  | '18'
  | '19'
  | '20'
  | '21'
  | '22'
  | '23';

export const dayMetricsSchema = v.object({
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

export type DayMetrics = v.Infer<typeof dayMetricsSchema>;

export type DayOfMonth =
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | '10'
  | '11'
  | '12'
  | '13'
  | '14'
  | '15'
  | '16'
  | '17'
  | '18'
  | '19'
  | '20'
  | '21'
  | '22'
  | '23'
  | '24'
  | '25'
  | '26'
  | '27'
  | '28'
  | '29'
  | '30'
  | '31';

export function yearMonth(date: Date): number {
  return date.getUTCFullYear() * 100 + (date.getUTCMonth() + 1);
}

export function splitDate(
  date: Date,
): [year: string, month: Month, dayOfMonth: DayOfMonth, hour: Hour] {
  const month = (date.getUTCMonth() + 1).toString();
  const mm = month.length > 1 ? month : '0' + month;
  return [
    date.getUTCFullYear().toString(),
    mm as Month,
    date.getUTCDate().toString() as DayOfMonth,
    date.getUTCHours().toString() as Hour,
  ];
}

// The MonthMetric sparsely tracks a month's worth of metrics for each hour of each day,
// with total-day and total-month aggregations. This schema is used to track both per-app
// metrics and per-team metrics.
//
// Nuance: Note that the `teamID` field is present for app-level metrics documents. Although
// this might seem redundant with the `teamID` in the App doc itself, keeping track of the
// `teamID` is necessary here because metrics documents are historic. In the event that
// we add support for transferring Apps to another team, the schema correctly attributes
// metrics to the team that the app belonged to when the metrics happened, and metrics for
// the App under the new team will be written to a different app-level document (the teamID
// is included in the app-level document ID). As a result, app-level metrics are partitioned
// across the teams that it has belonged to.
export const monthMetricsSchema = v.object({
  // The first three fields are designed to allow Collection Group queries across
  // teams (i.e. for our own global statistics) and across apps (i.e. for a team's breakdowns).
  //
  // Examples:
  // * Global statistics with team breakdown:
  //   `collectionGroup('metrics').where('appID', '==', null)`
  // * Global statistics with app breakdown:
  //   `collectionGroup('metrics').where('appID', '!=', null)`
  // * One team's apps:
  //   `collectionGroup('metrics').where('teamID', '==', teamID).where('appID', '!=', null)`
  // * One team's metrics with team aggregation and app breakdown:
  //   `collectionGroup('metrics').where('teamID', '==', teamID)`
  // * Time ranges: `.where(yearMonth, '>=', 202310)`
  teamID: v.string(),
  appID: v.string().nullable(), // null for Team-level metrics.
  yearMonth: v.number(),

  total: metricsSchema,

  // Note: The `day` field  contains a large subtree of fields that can
  // consume a lot of storage space for indexing. Since we don't anticipate
  // needing to sort queries by values in the day/hour range, we exclude
  // the whole tree from indexes in firestore.indexes.json.
  day: v.object({
    ['1']: dayMetricsSchema.optional(),
    ['2']: dayMetricsSchema.optional(),
    ['3']: dayMetricsSchema.optional(),
    ['4']: dayMetricsSchema.optional(),
    ['5']: dayMetricsSchema.optional(),
    ['6']: dayMetricsSchema.optional(),
    ['7']: dayMetricsSchema.optional(),
    ['8']: dayMetricsSchema.optional(),
    ['9']: dayMetricsSchema.optional(),
    ['10']: dayMetricsSchema.optional(),
    ['11']: dayMetricsSchema.optional(),
    ['12']: dayMetricsSchema.optional(),
    ['13']: dayMetricsSchema.optional(),
    ['14']: dayMetricsSchema.optional(),
    ['15']: dayMetricsSchema.optional(),
    ['16']: dayMetricsSchema.optional(),
    ['17']: dayMetricsSchema.optional(),
    ['18']: dayMetricsSchema.optional(),
    ['19']: dayMetricsSchema.optional(),
    ['20']: dayMetricsSchema.optional(),
    ['21']: dayMetricsSchema.optional(),
    ['22']: dayMetricsSchema.optional(),
    ['23']: dayMetricsSchema.optional(),
    ['24']: dayMetricsSchema.optional(),
    ['25']: dayMetricsSchema.optional(),
    ['26']: dayMetricsSchema.optional(),
    ['27']: dayMetricsSchema.optional(),
    ['28']: dayMetricsSchema.optional(),
    ['29']: dayMetricsSchema.optional(),
    ['30']: dayMetricsSchema.optional(),
    ['31']: dayMetricsSchema.optional(),
  }),
});

export type MonthMetrics = v.Infer<typeof monthMetricsSchema>;

export const monthMetricsDataConverter =
  firestoreDataConverter(monthMetricsSchema);

// TotalMetrics tracks the total and per-year aggregations of all metrics.
// Similarly to MonthMetrics, these are tracked per team and per app.
export const totalMetricsSchema = v.object({
  teamID: v.string(),
  appID: v.string().nullable(),

  total: metricsSchema,
  // Keyed by string, e.g. "2023"
  year: v.record(metricsSchema),
});

export type TotalMetrics = v.Infer<typeof totalMetricsSchema>;

export const totalMetricsDataConverter =
  firestoreDataConverter(totalMetricsSchema);

export const METRICS_COLLECTION_ID = 'metrics';

export function appMetricsCollection(appID: string): string {
  return path.append(appPath(appID), METRICS_COLLECTION_ID);
}

export function teamMetricsCollection(teamID: string): string {
  return path.append(teamPath(teamID), METRICS_COLLECTION_ID);
}

export type Month =
  | '01'
  | '02'
  | '03'
  | '04'
  | '05'
  | '06'
  | '07'
  | '08'
  | '09'
  | '10'
  | '11'
  | '12';

export function monthMetricsPath(
  year: string,
  month: Month,
  teamID: string,
  appID?: string,
): string {
  return metricsDocPath(teamID, appID, `${year}${month}`);
}

export function totalMetricsPath(teamID: string, appID?: string): string {
  return metricsDocPath(teamID, appID, 'total');
}

function metricsDocPath(
  teamID: string,
  appID: string | undefined,
  docPrefix: string,
) {
  const docSuffix = appID ? `-${teamID}` : '';
  return path.append(
    appID ? appPath(appID) : teamPath(teamID),
    METRICS_COLLECTION_ID,
    `${docPrefix}${docSuffix}`,
  );
}
