// This is taken from workers-sdk/packages/wrangler/
// TODO(arv): Remove thing we don not need.

import {logger} from 'firebase-functions';
import {assert} from 'shared/src/asserts.js';

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

function truncate(text: string, maxLength: number): string {
  const {length} = text;
  if (length <= maxLength) {
    return text;
  }
  return `${text.substring(0, maxLength)}... (length = ${length})`;
}

export async function cfFetch<ResponseType = unknown>(
  apiToken: string,
  resource: string,
  init: RequestInit = {},
  searchParams?: URLSearchParams,
): Promise<ResponseType> {
  assert(resource.startsWith('/'), 'resource must start with /');
  const base = 'https://api.cloudflare.com/client/v4';
  const queryString = searchParams ? `?${searchParams.toString()}` : '';

  const url = `${base}${resource}${queryString}`;

  const response = await fetch(url, {
    ...init,
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${apiToken}`,
      ...init?.headers,
    },
  });
  const jsonText = await response.text();
  logger.debug(`cfFetch: URL: ${url}`, 'response:', jsonText);

  const method = init.method ?? 'GET';

  let json;
  try {
    json = parseJSON<FetchResult<ResponseType>>(jsonText);
  } catch (err) {
    throw new ParseError({
      text: 'Received a malformed response from the API',
      notes: [
        {
          text: truncate(jsonText, 100),
        },
        {
          text: `${method} ${resource} -> ${response.status} ${response.statusText}`,
        },
      ],
    });
  }

  if (json.success) {
    return json.result;
  }
  throwFetchError(resource, json);
}

export type Location = File & {
  line: number;
  column: number;
  length?: number | undefined;
  lineText?: string | undefined;
  suggestion?: string | undefined;
};

export type File = {
  file?: string | undefined;
  fileText?: string | undefined;
};

function renderError(err: FetchError, level = 0): string {
  const chainedMessages =
    err.error_chain
      ?.map(
        chainedError =>
          `\n${'  '.repeat(level)}- ${renderError(chainedError, level + 1)}`,
      )
      .join('\n') ?? '';
  return (
    (err.code ? `${err.message} [code: ${err.code}]` : err.message) +
    chainedMessages
  );
}

function throwFetchError(
  resource: string,
  response: FetchResult<unknown>,
): never {
  const error = new ParseError({
    text: `A request to the Cloudflare API (${resource}) failed.`,
    notes: response.errors.map(err => ({
      text: renderError(err),
    })),
  });
  // add the first error code directly to this error
  // so consumers can use it for specific behaviour
  const code = response.errors[0]?.code;
  if (code) {
    //@ts-expect-error non-standard property on Error
    error.code = code;
  }
  throw error;
}

/**
 * Calculates the line and column location from an index.
 */
export function indexLocation(file: File, index: number): Location {
  let lineText,
    line = 0,
    column = 0,
    cursor = 0;
  const {fileText = ''} = file;
  for (const row of fileText.split('\n')) {
    line++;
    cursor += row.length + 1;
    if (cursor >= index) {
      lineText = row;
      column = row.length - (cursor - index);
      break;
    }
  }
  return {lineText, line, column, ...file};
}

const JSON_ERROR_SUFFIX = ' in JSON at position ';

/**
 * A wrapper around `JSON.parse` that throws a `ParseError`.
 */
export function parseJSON<T>(input: string, file?: string): T {
  try {
    return JSON.parse(input);
  } catch (err) {
    const {message} = err as Error;
    const index = message.lastIndexOf(JSON_ERROR_SUFFIX);
    if (index < 0) {
      throw err;
    }
    const text = message.substring(0, index);
    const position = parseInt(
      message.substring(index + JSON_ERROR_SUFFIX.length),
    );
    const location = indexLocation({file, fileText: input}, position);
    throw new ParseError({text, location});
  }
}

export type Message = {
  text: string;
  location?: Location | undefined;
  notes?: Message[] | undefined;
  kind?: 'warning' | 'error';
};

/**
 * An error that's thrown when something fails to parse.
 */
export class ParseError extends Error implements Message {
  readonly text: string;
  readonly notes: Message[];
  readonly location?: Location | undefined;
  readonly kind: 'warning' | 'error';

  constructor({text, notes, location, kind}: Message) {
    super(text);
    this.name = this.constructor.name;
    this.text = text;
    this.notes = notes ?? [];
    this.location = location;
    this.kind = kind ?? 'error';
  }
}
