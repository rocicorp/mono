import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';

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

  return fetchWithRetries(url, {
    ...init,
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${apiToken}`,
      ...init?.headers,
    },
  });
}

// Backoff parameters are chosen to emulate Cloudflare's policy for
// Durable Object Alarm retries. Arbitrary but reasonable.
// https://developers.cloudflare.com/durable-objects/api/alarms/
//
// The resulting maximum delay is around 42 seconds.
const INITIAL_RETRY_DELAY = 2000;
const BACKOFF = 1.5;
const MAX_ATTEMPTS = 6;

async function fetchWithRetries(
  url: string,
  init: RequestInit,
  attempt = 0,
): Promise<Response> {
  const resp = await fetch(url, init);
  if (resp.status < 500 || attempt >= MAX_ATTEMPTS) {
    return resp;
  }
  console.error(
    `${resp.status}: ${resp.statusText} (${init.method ?? 'GET'} ${url})`,
  );
  const timeout = INITIAL_RETRY_DELAY * BACKOFF ** attempt;
  console.info(`Attempt ${attempt}: retrying after ${timeout / 1000} seconds`);

  await sleep(timeout);
  return fetchWithRetries(url, init, attempt + 1);
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
    const err = new FetchResultError(json, action);
    err.stack = insertStack(err.stack ?? '', json.errors);
    throw err;
  }
  throw new Error(`Error returned for ${action}: ${JSON.stringify(json)}`);
}

export class FetchResultError extends Error implements FetchError {
  static throwIfCodeIsNot(e: unknown, ...codes: [number, ...number[]]) {
    if (e instanceof FetchResultError && codes.includes(e.code)) {
      console.debug(`Allowed Error Code ${e.code}`, e);
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
        result.messages?.length ? result.messages[0] : JSON.stringify(result)
      }`,
    );
    assert(result.success === false);

    this.code = result.errors.length ? result.errors[0].code : 0;
    this.error_chain = result.errors;
  }

  codes(): number[] {
    return this.error_chain.map(error => error.code);
  }

  messages(): string[] {
    return this.error_chain.map(error => error.message);
  }
}

interface FetchError {
  code: number;
  message: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  error_chain?: FetchError[];
}

export interface FetchResult<ResponseType = unknown> {
  success: boolean;
  result: ResponseType;
  errors: FetchError[];
  messages?: string[];
  // eslint-disable-next-line @typescript-eslint/naming-convention
  result_info?: unknown;
}

export enum Errors {
  TooManyRequests = 971,
  CouldNotRouteToScript = 7003,
  CustomHostnameNotFound = 1436,
  DispatchNamespaceNotFound = 100119,
  DuplicateCustomHostnameFound = 1406,
  EnvironmentNotFound = 10092,
  RecordAlreadyExists = 81057,
  RecordWithHostAlreadyExists = 81053, // A, AAAA, CNAME collision
  RecordDoesNotExist = 81044,
  ResourceNotFound = 1551,
  ScriptNotFound = 10007,
  ScriptContentFailedValidationChecks = 10021,
  ScriptBodyWasTooLarge = 10027,
  ServiceNotFound = 10090,
}

function insertStack(orig: string, errors: FetchError[]): string {
  const [first, rest] = splitFirstLine(orig);
  let stack = first + '\n';
  for (const {code, message} of errors) {
    const [first, rest] = splitFirstLine(message);
    stack += `  at ${first} (cloudflare:${code})\n`;
    if (rest) {
      stack += rest.endsWith('\n') ? rest : `${rest}\n`;
    }
  }
  return stack + rest;
}

function splitFirstLine(orig: string): [first: string, rest: string] {
  const newline = orig.indexOf('\n');
  const first = newline >= 0 ? orig.substring(0, newline) : orig;
  const rest = newline >= 0 ? orig.substring(newline + 1) : '';
  return [first, rest];
}
