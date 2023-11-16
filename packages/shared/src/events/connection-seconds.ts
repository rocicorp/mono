import * as v from '../valita.js';

// Increment when making non-backwards compatible changes to the schema.
const SCHEMA_VERSION = 2;

export const connectionSecondsReportSchema = v.object({
  /** Reporting period, in seconds. */
  period: v.number(),

  /**
   * Connection-seconds elapsed during the interval.
   * It follows that `elapsed / interval` is equal to the
   * average number of connections during the interval.
   */
  elapsed: v.number(),

  /** Room ID of the connection. */
  roomID: v.string(),
});

export type ConnectionSecondsReport = v.Infer<
  typeof connectionSecondsReportSchema
>;

export const CONNECTION_SECONDS_CHANNEL_NAME = `connection-seconds@v${SCHEMA_VERSION}`;

// Historic schemas for processing old Workers.
export const CONNECTION_SECONDS_V1_CHANNEL_NAME = `connection-seconds@v1`;
export const connectionSecondsReportV1Schema = v.object({
  /** Reporting interval, in seconds. */
  interval: v.number(),

  /**
   * Connection-seconds elapsed during the interval.
   * It follows that `elapsed / interval` is equal to the
   * average number of connections during the interval.
   */
  elapsed: v.number(),
});
