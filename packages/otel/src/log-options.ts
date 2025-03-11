import * as v from '../../shared/src/valita.ts';
import {type Config} from '../../shared/src/options.ts';

export const logOptions = {
  level: v
    .union(
      v.literal('debug'),
      v.literal('info'),
      v.literal('warn'),
      v.literal('error'),
    )
    .default('info'),

  format: {
    type: v.union(v.literal('text'), v.literal('json')).default('text'),
    desc: [
      `Use {bold text} for developer-friendly console logging`,
      `and {bold json} for consumption by structured-logging services`,
    ],
  },

  traceCollector: {
    type: v.string().optional(),
    desc: [
      `The URL of the trace collector to which to send trace data. Traces are sent over http.`,
      `Port defaults to 4318 for most collectors.`,
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
