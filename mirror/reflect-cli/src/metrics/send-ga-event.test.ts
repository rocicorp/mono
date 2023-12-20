import {expect, jest, test, afterEach} from '@jest/globals';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
const fetch = new FetchMocker().result(
  'POST',
  'https://www.google-analytics.com/g/collect',
  [],
);
import {sendGAEvent} from './send-ga-event.js';
import {authContext} from '../login.test.helper.js';
import type {AuthenticatedUser} from '../auth-config.js';

afterEach(() => {
  jest.restoreAllMocks();
});

test('send-ga-event', async () => {
  try {
    await sendGAEvent(
      [{en: 'event-name'}],
      authContext?.user as AuthenticatedUser,
    );
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
  expect(reqs[0][1]).toContain('uid=fake-uid');
});
