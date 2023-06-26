import type {FetchResponse} from '@rocicorp/licensing/src/client';

// mustSimpleFetch throws on non-2xx responses.
export async function mustSimpleFetch(
  method: string,
  url: string,
  body: string | null,
  headers: Record<string, string>,
): Promise<FetchResponse> {
  const resp = await fetch(url, {method, body, headers});
  if (!resp.ok) {
    throw new Error(`Got ${resp.status} fetching ${url}: ${await resp.text()}`);
  }
  return resp;
}
