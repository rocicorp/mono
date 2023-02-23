import type {HTTPRequestInfo} from './http-request-info.js';

/**
 * Helper function for {@link getDefaultPuller} and {@link getDefaultPusher}.
 */
export async function callDefaultFetch<Body>(
  url: string,
  auth: string,
  requestID: string,
  requestBody: Body,
): Promise<readonly [Response | undefined, HTTPRequestInfo]> {
  const init = {
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-type': 'application/json',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Authorization': auth,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'X-Replicache-RequestID': requestID,
    },
    body: JSON.stringify(requestBody),
    method: 'POST',
  };
  const request = new Request(url, init);
  const response = await fetch(request);
  const httpStatusCode = response.status;
  if (httpStatusCode !== 200) {
    return [
      undefined,
      {
        httpStatusCode,
        errorMessage: await response.text(),
      },
    ];
  }
  return [
    response,
    {
      httpStatusCode,
      errorMessage: '',
    },
  ];
}
