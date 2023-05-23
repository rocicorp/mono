import type {FetchResponse} from '@rocicorp/licensing/src/client';

// This wrapper around fetch adapts it to the narrow fetch interface
// that the licensing client expects. That type enables the licensing
// client to work with both the browser and node-fetch implementations.
function browserSimpleFetch(
  method: string,
  url: string,
  body: string | null,
  headers: [string, string][],
): Promise<FetchResponse> {
  const requestInit = {
    method,
    body,
    headers,
  };
  return fetch(url, requestInit);
}

// mustSimpleFetch throws on non-200 responses.
export async function mustSimpleFetch(
  method: string,
  url: string,
  body: string | null,
  headers: Record<string, string>,
): Promise<FetchResponse> {
  const resp = await browserSimpleFetch(
    method,
    url,
    body,
    Object.entries(headers),
  );
  if (resp.status !== 200) {
    throw new Error(`Got ${resp.status} fetching ${url}: ${await resp.text()}`);
  }
  return resp;
}
