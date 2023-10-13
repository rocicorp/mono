import * as v from 'shared/src/valita.js';
import {
  CONNECTION_SECONDS_CHANNEL_NAME,
  connectionSecondsReportSchema as reportSchema,
} from 'shared/src/events/connection-seconds.js';
import {type ScriptTags, parseScriptTags} from '../script-tags.js';

export interface Env {
  // blob1  | blob2 | double1 | double2  | timestamp
  // -----------------------------------------------------------
  // teamID | appID | elapsed | interval | (report time)
  runningConnectionSecondsDS: AnalyticsEngineDataset;

  // blob1  | blob2 | double1    | double2  | timestamp
  // -------------------------------------------------------------------------
  // teamID | appID | start-time | end-time | (report time should == end-time)
  connectionLifetimesDS: AnalyticsEngineDataset;
}

function reportConnectionSeconds(
  runningConnectionSecondsDS: AnalyticsEngineDataset,
  tags: ScriptTags,
  diagnosticChannelMessage: unknown,
) {
  const report = v.parse(diagnosticChannelMessage, reportSchema); // Note: 'strict'
  if (report.elapsed <= 0 || report.interval <= 0) {
    console.warn(
      `Suspicious ConnectionSecondsReport from ${tags.appID} (${tags.appName}.${tags.teamLabel})`,
      report,
    );
    return;
  }
  runningConnectionSecondsDS.writeDataPoint({
    blobs: [tags.teamID, tags.appID],
    doubles: [report.elapsed, report.interval],
  });
  console.info(
    `Reported connection seconds for ${tags.appName}.${tags.teamLabel}`,
    report,
  );
}

// To make tests easier to mock, be explicit about which fields we read.
type TailItem = Pick<
  TraceItem,
  'scriptTags' | 'diagnosticsChannelEvents' | 'eventTimestamp' | 'event'
>;

function reportRunningConnectionElapsedSeconds(
  events: TailItem[],
  runningConnectionSecondsDS: AnalyticsEngineDataset,
) {
  for (const {scriptTags, diagnosticsChannelEvents} of events) {
    if (diagnosticsChannelEvents.length === 0) {
      continue; // Optimization: Avoid parsing script tags.
    }
    let tags: ScriptTags;
    try {
      tags = parseScriptTags(scriptTags ?? []);
    } catch (e) {
      console.error(`Missing expected script tags: ${String(e)}`, scriptTags);
      continue;
    }
    for (const e of diagnosticsChannelEvents) {
      if (e.channel === CONNECTION_SECONDS_CHANNEL_NAME) {
        try {
          reportConnectionSeconds(runningConnectionSecondsDS, tags, e.message);
        } catch (e) {
          console.error(`Invalid ConnectionSecondsReport: ${String(e)}`, e);
        }
      }
    }
  }
}

export const AUTH_DATA_HEADER_NAME = 'x-reflect-auth-data';

function reportConnectionLifetimes(
  events: TailItem[],
  connectionLifetimesDS: AnalyticsEngineDataset,
) {
  const endTime = Date.now();
  for (const {scriptTags, event, eventTimestamp} of events) {
    // Test if this is a FetchEvent received by the RoomDO. This is determined by the
    // presence of the "x-reflect-auth-data" header. This effectively filters out the
    // duplicate FetchEvents created by the intervening WorkerRouter and AuthDO, as
    // well as unrelated FetchEvents like metrics reports.
    const fetch = event as TraceItemFetchEventInfo;
    const authData = fetch?.request?.headers?.[AUTH_DATA_HEADER_NAME];
    if (!authData) {
      continue;
    }
    let tags: ScriptTags;
    try {
      tags = parseScriptTags(scriptTags ?? []);
    } catch (e) {
      console.error(`Missing expected script tags: ${String(e)}`, scriptTags);
      continue;
    }
    if (!eventTimestamp) {
      console.error(`Missing eventTimestamp in FetchEvent`);
      continue;
    }
    connectionLifetimesDS.writeDataPoint({
      blobs: [tags.teamID, tags.appID],
      doubles: [eventTimestamp, endTime],
    });
    console.info(
      `Reported connection lifetime for ${tags.appName}.${tags.teamLabel} (${
        (endTime - eventTimestamp) / 1000
      } seconds)`,
    );
  }
}

export default {
  tail(events: TailItem[], env: Env) {
    reportConnectionLifetimes(events, env.connectionLifetimesDS);

    reportRunningConnectionElapsedSeconds(
      events,
      env.runningConnectionSecondsDS,
    );
  },
};
