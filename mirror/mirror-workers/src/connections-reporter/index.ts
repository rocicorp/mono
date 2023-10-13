import * as v from 'shared/src/valita.js';
import {
  CONNECTION_SECONDS_CHANNEL_NAME,
  connectionSecondsReportSchema as reportSchema,
} from 'shared/src/events/connection-seconds.js';
import {type ScriptTags, parseScriptTags} from '../script-tags.js';

export interface Env {
  // blob1  | blob2 | double1 | double2
  // -----------------------------------
  // teamID | appID | elapsed | interval
  runningConnectionSecondsDS: AnalyticsEngineDataset;
}

function reportConnectionSeconds(
  runningConnectionSecondsDS: AnalyticsEngineDataset,
  scriptTags: ScriptTags,
  diagnosticChannelMessage: unknown,
) {
  const report = v.parse(diagnosticChannelMessage, reportSchema); // Note: 'strict'
  if (report.elapsed <= 0 || report.interval <= 0) {
    console.warn(
      `Suspicious ConnectionSecondsReport from ${scriptTags.appID} (${scriptTags.appName}.${scriptTags.teamLabel})`,
      report,
    );
    return;
  }
  runningConnectionSecondsDS.writeDataPoint({
    blobs: [scriptTags.teamID, scriptTags.appID],
    doubles: [report.elapsed, report.interval],
  });
}

// To make tests easier to mock, be explicit about which fields we read.
type TailItem = Pick<TraceItem, 'scriptTags' | 'diagnosticsChannelEvents'>;

export default {
  tail(events: TailItem[], env: Env) {
    for (const {scriptTags, diagnosticsChannelEvents} of events) {
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
            reportConnectionSeconds(
              env.runningConnectionSecondsDS,
              tags,
              e.message,
            );
          } catch (e) {
            console.error(`Invalid ConnectionSecondsReport: ${String(e)}`, e);
          }
        }
      }
    }
  },
};
