import * as v from 'shared/src/valita.js';
import type {ListOptions} from '../storage/storage.js';

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

export const listParamsSchema = v
  .object({
    startKey: v.string().optional(),
    startAfterKey: v.string().optional(),
    maxResults: numericString.optional(),
  })
  .assert(
    v => v.startKey === undefined || v.startAfterKey === undefined,
    'Cannot specify both startKey and startAfterKey',
  );

export type ListParams = v.Infer<typeof listParamsSchema>;

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
  listParams: ListParams,
  maxMaxResults: number,
): ListControl {
  const {
    maxResults: requestedMaxResults = maxMaxResults,
    startKey = '',
    startAfterKey,
  } = listParams;
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
