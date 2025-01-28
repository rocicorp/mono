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
    type: v.number().default(3),
    desc: [
      `The number of ms a row must take to fetch from table-source before it is considered slow.`,
    ],
  },

  ivmSampling: {
    type: v.number().default(0),
    desc: [
      `How often to take collect IVM metrics. 1 means always, 100 means 1% of the time, 0 means never`,
    ],
  },
};

export type LogConfig = Config<typeof logOptions>;
