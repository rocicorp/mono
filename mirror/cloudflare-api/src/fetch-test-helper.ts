import {FetchMocker, SpyOn} from 'shared/src/fetch-mocker.js';
import type {FetchResult} from './fetch.js';

export function mockFetch(spyOn: SpyOn): FetchMocker {
  return new FetchMocker(spyOn, success, error);
}

function success<T>(result: T): Response {
  const fetchResult: FetchResult<T> = {
    success: true,
    result,
    errors: [],
    messages: [],
  };
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(fetchResult),
  } as unknown as Response;
}

function error(code: number, message?: string): Response {
  const fetchResult: FetchResult<null> = {
    success: false,
    result: null,
    errors: [{code, message: message ?? `Error code ${code}`}],
    messages: [],
  };
  return {
    ok: true,
    status: 400,
    json: () => Promise.resolve(fetchResult),
  } as unknown as Response;
}
