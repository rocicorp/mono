import {getProviderConfig} from './cf.js';
import {cfCall} from 'cloudflare-api/src/fetch.js';
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
  const {apiToken, accountID} = await getProviderConfig(yargs);
  const resource = `/accounts/${accountID}/analytics_engine/sql`;
  const {query} = yargs;
  const resp = await cfCall(apiToken, resource, {
    method: 'POST',
    body: query,
  });
  console.log(`${query}:`, await resp.text());
}
