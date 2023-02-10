import type {HTTPRequestInfo} from './http-request-info';

/**
 * This is used by the defaultPuller and defaultPusher to reduce code
 * duplication.
 */
export async function httpRequest(
  request: Request,
): Promise<{httpRequestInfo: HTTPRequestInfo; response: Response}> {
  const response = await fetch(request);
  const httpStatusCode = response.status;
  const errorMessage = httpStatusCode === 200 ? '' : await response.text();
  return {
    response,
    httpRequestInfo: {
      httpStatusCode,
      errorMessage,
    },
  };
}
