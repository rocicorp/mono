import {type Config} from '../../shared/src/options-types.ts';
import * as v from '../../shared/src/valita.ts';

export const logLevel = v.literalUnion('debug', 'info', 'warn', 'error');

export const logOptions = {
  level: logLevel.default('info'),

  format: {
    type: v.literalUnion('text', 'json').default('text'),
    desc: [
      `Use {bold text} for developer-friendly console logging`,
      `and {bold json} for consumption by structured-logging services`,
    ],
  },

  slowRowThreshold: {
    type: v.number().default(2),
    desc: [
      `The number of ms a row must take to fetch from table-source before it is considered slow.`,
    ],
  },

  slowHydrateThreshold: {
    type: v.number().default(100),
    desc: [
      `The number of milliseconds a query hydration must take to print a slow warning.`,
      ``,
      `The warning logs the query with its literal values redacted, and is logged`,
      `at most once every 5 minutes per query shape (the query with its values`,
      `redacted), with a count of the slow hydrations suppressed in between.`,
    ],
  },

  planWarningRowThreshold: {
    type: v.number().default(10_000),
    desc: [
      `Log a warning when the query planner estimates that one read of a table`,
      `scans or sorts at least this many rows: a read that scans the whole`,
      `table because no index covers the columns it looks rows up by, or that`,
      `sorts every matching row because no index covers the ordering.`,
      ``,
      `The warning is logged at most once an hour per query shape (the query`,
      `with its values redacted). Set to 0 to disable. Requires the query planner.`,
    ],
  },

  planWarningCostThreshold: {
    type: v.number().default(1_000_000),
    desc: [
      `Log a warning when the best plan the query planner finds for a query is`,
      `still estimated to process at least this many rows. Throttled like`,
      `{bold planWarningRowThreshold}. Set to 0 to disable.`,
    ],
  },

  ivmSampling: {
    type: v.number().default(5000),
    desc: [
      `How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.`,
    ],
  },
};

export type LogConfig = Config<typeof logOptions>;
