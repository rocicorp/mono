import {assert} from 'shared/src/asserts.js';

export function cfCall(
  apiToken: string,
  resource: string,
  init: RequestInit = {},
  searchParams?: URLSearchParams,
): Promise<Response> {
  assert(resource.startsWith('/'), 'resource must start with /');
  const base = 'https://api.cloudflare.com/client/v4';
  const queryString = searchParams ? `?${searchParams.toString()}` : '';

  const url = `${base}${resource}${queryString}`;

  return fetch(url, {
    ...init,
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${apiToken}`,
      ...init?.headers,
    },
  });
}

export async function cfFetch<ResponseType = unknown>(
  apiToken: string,
  resource: string,
  init: RequestInit = {},
  searchParams?: URLSearchParams,
): Promise<ResponseType> {
  const resp = await cfCall(apiToken, resource, init, searchParams);
  const action = `${init.method ?? 'GET'} ${resource}`;
  let json;
  try {
    json = (await resp.json()) as FetchResult<ResponseType>;
  } catch (e) {
    throw new Error(
      `${action}: ${resp.status}: ${resp.statusText}: ${String(e)}`,
    );
  }
  if (json.success) {
    return json.result;
  }
  if (json.errors?.length) {
    throw new FetchResultError(json, action);
  }
  throw new Error(`Error returned for ${action}: ${JSON.stringify(json)}`);
}

export class FetchResultError extends Error implements FetchError {
  static throwIfCodeIsNot(e: unknown, ...codes: [number, ...number[]]) {
    if (e instanceof FetchResultError && codes.includes(e.code)) {
      return;
    }
    throw e;
  }

  readonly code: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly error_chain: FetchError[];

  constructor(result: FetchResult, action: string) {
    super(
      `${action}: ${
        result.messages.length > 0 ? result.messages[0] : JSON.stringify(result)
      }`,
    );
    assert(result.success === false);

    this.code = result.errors.length ? result.errors[0].code : 0;
    this.error_chain = result.errors;
  }

  codes(): number[] {
    return this.error_chain.map(error => error.code);
  }
}

interface FetchError {
  code: number;
  message: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  error_chain?: FetchError[];
}

interface FetchResult<ResponseType = unknown> {
  success: boolean;
  result: ResponseType;
  errors: FetchError[];
  messages: string[];
  // eslint-disable-next-line @typescript-eslint/naming-convention
  result_info?: unknown;
}

export const ERRORS = {
  couldNotRouteToScript: 7003,
  dispatchNamespaceNotFound: 100119,
  environmentNotFound: 10092,
  recordAlreadyExists: 81057,
  resourceNotFound: 1551,
  scriptNotFound: 10007,
  serviceNotFound: 10090,
} as const;
