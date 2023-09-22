import {logger} from 'firebase-functions';
import type {GlobalScript} from 'cloudflare-api/src/scripts.js';

export function submitTriggers(script: GlobalScript, cron: string) {
  logger.log('Setting up triggers:', cron);
  return script.setSchedules([{cron}]);
}
