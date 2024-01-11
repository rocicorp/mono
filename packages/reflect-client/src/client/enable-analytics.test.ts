import {expect} from 'chai';
import {shouldEnableAnalytics} from './enable-analytics.js';

suite('when server indicates testing or local dev', () => {
  const cases: (string | null)[] = [
    null,
    'http://localhost',
    'http://localhost:8000',
    'http://127.0.0.1',
    'http://127.0.0.1:1900',
    'https://[2001:db8:3333:4444:5555:6666:7777:8888]:9000',
  ];
  for (const c of cases) {
    test(c + '', () => {
      expect(shouldEnableAnalytics(c, true)).false;
      expect(shouldEnableAnalytics(c, false)).false;
      expect(shouldEnableAnalytics(c)).false;
    });
  }
});

test('enableAnalytics true and server does not indicate testing or local dev', () => {
  expect(shouldEnableAnalytics('https://subdomain.domain.net', true)).true;
});

test('enableAnalytics undefined and server does not indicate testing or local dev', () => {
  expect(shouldEnableAnalytics('https://subdomain.domain.net', undefined)).true;
  expect(shouldEnableAnalytics('https://subdomain.domain.net')).true;
});
