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
  if (!resp.ok) {
    throw new Error(`${resp.status}: ${resp.statusText}`);
  }
  const json = (await resp.json()) as FetchResult<ResponseType>;
  if (json.success) {
    return json.result;
  }
  if (json.errors?.length) {
    throw new FetchResultError(json);
  }
  throw new Error(`Error returned for ${resource}: ${JSON.stringify(json)}`);
}

export class FetchResultError extends Error implements FetchError {
  readonly code: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly error_chain: FetchError[];

  constructor(result: FetchResult) {
    super(
      result.messages.length > 0 ? result.messages[0] : JSON.stringify(result),
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
