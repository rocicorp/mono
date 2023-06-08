import * as functions from 'firebase-functions';

/**
 * Healthcheck function.
 */
export function healthcheck(
  _request: functions.Request,
  response: functions.Response,
): void {
  const result = JSON.stringify({message: 'ok'});
  response.status(200);
  response.send(result);
}
