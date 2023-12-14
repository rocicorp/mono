import * as v from 'shared/src/valita.js';
import type {ListOptions} from '../storage/storage.js';
import {HttpError} from './errors.js';

const numericString = v.string().chain(str => {
  try {
    const val = parseInt(str);
    if (Number.isNaN(val)) {
      return v.err('Expected valid number');
    }
    return v.ok(val);
  } catch (e) {
    return v.err(String(e));
  }
});

const listParamsSchema = v
  .object({
    startKey: v.string().optional(),
    startAfterKey: v.string().optional(),
    maxResults: numericString.optional(),
  })
  .assert(
    v => v.startKey === undefined || v.startAfterKey === undefined,
    'Cannot specify both startKey and startAfterKey',
  );

function queryStringToObj<T>(queryString: string, schema: v.Type<T>): T {
  const queryObj = Object.fromEntries(
    new URLSearchParams(queryString).entries(),
  );
  try {
    return v.parse(queryObj, schema, 'passthrough');
  } catch (e) {
    throw new HttpError(400, (e as Error).message);
  }
}

export type ListResults<T> = {
  results: T[];
  numResults: number;
  more: boolean;
};

export interface ListControl {
  getOptions(): ListOptions;
  makeListResults<T>(fetched: Iterable<T>): ListResults<T>;
}

export function makeListControl(
  queryString: string,
  maxMaxResults: number,
): ListControl {
  const {
    maxResults: requestedMaxResults = maxMaxResults,
    startKey = '',
    startAfterKey,
  } = queryStringToObj(queryString, listParamsSchema);
  const maxResults = Math.min(requestedMaxResults, maxMaxResults);

  return {
    getOptions: () => ({
      start: {
        key: startAfterKey ?? startKey,
        exclusive: startAfterKey !== undefined,
      },
      limit: maxResults + 1, // Fetch 1 more than maxResults to determine `more`.
    }),
    makeListResults: <T>(fetched: Iterable<T>) =>
      makeListResults(fetched, maxResults),
  };
}

export function makeListResults<T>(fetchedIt: Iterable<T>, maxResults: number) {
  const fetched = Array.from(fetchedIt);
  const more = fetched.length > maxResults;
  return {
    results: more ? fetched.slice(0, maxResults) : fetched,
    numResults: more ? maxResults : fetched.length,
    more,
  };
}
