import * as functions from 'firebase-functions';

/**
 * Heartbeat function.
 */
export function heartbeat(
  request: functions.Request,
  response: functions.Response,
): void {
  const result = JSON.stringify({message: 'ok'});
  response.status(200);
  response.send(result);
}
