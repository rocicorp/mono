import {expect, jest, test, afterEach} from '@jest/globals';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
const fetch = new FetchMocker(success, error).result(
  'POST',
  'https://www.google-analytics.com/g/collect',
  [],
);
import {sendGAEvent} from './send-ga-event.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('send-ga-event', async () => {
  try {
    await sendGAEvent([{en: 'event-name'}]);
  } catch (e) {
    console.log(e);
  }
  const reqs = fetch.requests();
  const bodys = fetch.bodys();

  expect(bodys.length).toEqual(1);
  expect(bodys[0]).toContain('en=event-name');
  expect(reqs.length).toEqual(1);
  expect(reqs[0][1]).toContain(
    'uamb=0&seg=1&uafvl=Google%2520Chrome%3B111.0.5563.64%7CNot(A%253ABrand%3B8.0.0.0%7CChromium%3B111.0.5563.64',
  );
});

function success<T>(result: T): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(result),
  } as unknown as Response;
}

function error(code: number, message?: string): Response {
  const fetchResult = {
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
