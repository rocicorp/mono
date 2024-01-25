import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {appPath} from './deployment.js';
import * as path from './path.js';
import {teamPath} from './team.js';
import {timestampSchema} from './timestamp.js';

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

export const metricsNode = v.object({total: metricsSchema});

/**
 * MetricsNode defines the structure common to both intermediate and leaf
 * nodes of Metrics documents.
 */
export type MetricsNode = v.Infer<typeof metricsNode>;

/** Creates a union type from `0` to `Length - 1`. */
type Range<
  Length extends number,
  Result extends number[] = [],
> = Result['length'] extends Length
  ? Result[number]
  : Range<Length, [...Result, Result['length']]>;

/** Creates a union type from `Start` to `End`. */
type InclusiveRange<Start extends number, End extends number> =
  | Exclude<Range<End>, Range<Start>>
  | End;

export type Hour = `${Range<24>}`;

export const dayMetricsSchema = v.object({
  ...metricsNode.shape,
  hour: v.object({
    ['0']: metricsNode.optional(),
    ['1']: metricsNode.optional(),
    ['2']: metricsNode.optional(),
    ['3']: metricsNode.optional(),
    ['4']: metricsNode.optional(),
    ['5']: metricsNode.optional(),
    ['6']: metricsNode.optional(),
    ['7']: metricsNode.optional(),
    ['8']: metricsNode.optional(),
    ['9']: metricsNode.optional(),
    ['10']: metricsNode.optional(),
    ['11']: metricsNode.optional(),
    ['12']: metricsNode.optional(),
    ['13']: metricsNode.optional(),
    ['14']: metricsNode.optional(),
    ['15']: metricsNode.optional(),
    ['16']: metricsNode.optional(),
    ['17']: metricsNode.optional(),
    ['18']: metricsNode.optional(),
    ['19']: metricsNode.optional(),
    ['20']: metricsNode.optional(),
    ['21']: metricsNode.optional(),
    ['22']: metricsNode.optional(),
    ['23']: metricsNode.optional(),
  }),
});

export type DayMetrics = v.Infer<typeof dayMetricsSchema>;

export type DayOfMonth = `${InclusiveRange<1, 31>}`;

export function yearMonth(date: Date): number {
  return date.getUTCFullYear() * 100 + (date.getUTCMonth() + 1);
}

export function splitDate(
  date: Date,
): [year: string, month: Month, dayOfMonth: DayOfMonth, hour: Hour] {
  return [
    date.getUTCFullYear().toString(),
    (date.getUTCMonth() + 1).toString() as Month,
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

  ...metricsNode.shape,
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

export const yearMetricsSchema = v.object({
  ...metricsNode.shape,
  month: v.object({
    ['1']: metricsNode.optional(),
    ['2']: metricsNode.optional(),
    ['3']: metricsNode.optional(),
    ['4']: metricsNode.optional(),
    ['5']: metricsNode.optional(),
    ['6']: metricsNode.optional(),
    ['7']: metricsNode.optional(),
    ['8']: metricsNode.optional(),
    ['9']: metricsNode.optional(),
    ['10']: metricsNode.optional(),
    ['11']: metricsNode.optional(),
    ['12']: metricsNode.optional(),
  }),
});

export type YearMetrics = v.Infer<typeof yearMetricsSchema>;

// TotalMetrics sparsely tracks all-time totals as well as yearly and monthly breakdowns.
// Like MonthMetrics, this TotalMetrics schema is used to track both per-app
// metrics and per-team metrics.
//
// Note that this schema is technically unbounded because it holds an arbitrary number
// of years, but it will be decades before we approach the 1MB limit. In the event that
// this becomes an issue, the mitigation would be to:
// - implement a policy of discarding old years, or
// - archive old years to a different location, or
// - remove the month data from the totals doc entirely and query month documents instead.
//   (which is no doubt more cumbersome, but doable).
export const totalMetricsSchema = v.object({
  teamID: v.string(),
  appID: v.string().nullable(),
  yearMonth: v.null(),

  ...metricsNode.shape,
  year: v.record(yearMetricsSchema), // Keyed by string, e.g. "2023"
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

export type Month = `${InclusiveRange<1, 12>}`;

export function monthMetricsPath(
  year: string,
  month: Month,
  teamID: string,
  appID?: string | null,
): string {
  const mm = month.length > 1 ? month : `0${month}`;
  return metricsDocPath(teamID, appID, `${year}${mm}`);
}

export function totalMetricsPath(
  teamID: string,
  appID?: string | null,
): string {
  return metricsDocPath(teamID, appID, 'total');
}

function metricsDocPath(
  teamID: string,
  appID: string | null | undefined,
  docPrefix: string,
) {
  const docSuffix = appID ? `-${teamID}` : '';
  return path.append(
    appID ? appPath(appID) : teamPath(teamID),
    METRICS_COLLECTION_ID,
    `${docPrefix}${docSuffix}`,
  );
}

// Records when aggregations are attempted so that they can be retried later
// in the case of prolonged Cloudflare analytics outages.
export const aggregationSchema = v.object({
  lastAttempt: timestampSchema,
});

export type Aggregation = v.Infer<typeof aggregationSchema>;

export const aggregationDataConverter =
  firestoreDataConverter(aggregationSchema);

export const AGGREGATIONS = 'outstandingAggregations';

export function aggregationPath(endTime: Date): string {
  return path.join(AGGREGATIONS, endTime.toISOString());
}
