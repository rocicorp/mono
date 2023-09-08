import {cfCall, getCloudflareConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function queryAnalyticsOptions(yargs: CommonYargsArgv) {
  return yargs.positional('query', {
    desc: 'SQL query to run',
    type: 'string',
    demandOption: true,
  });
}

type QueryAnalyticsHandlerArgs = YargvToInterface<
  ReturnType<typeof queryAnalyticsOptions>
>;

// https://developers.cloudflare.com/analytics/analytics-engine/sql-api/
export async function queryAnalyticsHandler(yargs: QueryAnalyticsHandlerArgs) {
  const config = await getCloudflareConfig(yargs);
  const resource = `/accounts/${config.accountID}/analytics_engine/sql`;
  const {query} = yargs;
  const resp = await cfCall(config, resource, {
    method: 'POST',
    body: query,
  });
  console.log(`${query}:`, resp);
}
