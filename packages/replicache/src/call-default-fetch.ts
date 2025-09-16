/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {HTTPRequestInfo} from './http-request-info.ts';

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
