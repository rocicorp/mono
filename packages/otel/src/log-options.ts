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
