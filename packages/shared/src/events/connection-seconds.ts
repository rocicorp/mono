import * as v from '../valita.js';

// Increment when making non-backwards compatible changes to the schema.
const SCHEMA_VERSION = 1;

export const connectionSecondsReportSchema = v.object({
  /** Reporting interval, in seconds. */
  interval: v.number(),

  /**
   * Connection-seconds elapsed during the interval.
   * It follows that `elapsed / interval` is equal to the
   * average number of connections during the interval.
   */
  elapsed: v.number(),
});

export type ConnectionSecondsReport = v.Infer<
  typeof connectionSecondsReportSchema
>;

export const CONNECTION_SECONDS_CHANNEL_NAME = `connection-seconds@v${SCHEMA_VERSION}`;
