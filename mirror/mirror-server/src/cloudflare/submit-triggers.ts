import {cfFetch} from './cf-fetch.js';
import type {Config} from './config.js';

export function submitTriggers(config: Config, cron: string) {
  console.log('Setting up triggers:', cron);
  const {accountID, apiToken, scriptName} = config;
  return cfFetch(
    apiToken,
    `/accounts/${accountID}/workers/scripts/${scriptName}/schedules`,
    {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify([
        {
          cron,
        },
      ]),
    },
  );
}
