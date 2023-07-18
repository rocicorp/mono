import {afterEach, beforeEach, expect, jest, test} from '@jest/globals';
import {newAppID, newAppScriptName} from './ids.js';

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
});

test('appID is timestamp base62 encoded', () => {
  jest.setSystemTime(0);
  expect(newAppID()).toBe('0');

  jest.setSystemTime(1234567890);
  expect(newAppID()).toBe('1LY7VK');
});

test('script name', () => {
  const appID = '1LY7VK';
  let i = 0;
  const values = Array.from({length: 10}, (_, i) => i / 10);
  jest
    .spyOn(Math, 'random')
    .mockImplementation(() => values[i++ % values.length]);
  expect(newAppScriptName(appID)).toBe('aback-broad-cockatoo-14pc0mi');
});

test('script name not the same', () => {
  const appID = '1LY7VK';
  let i = 0;
  const values = [
    ...Array.from({length: 10}, () => 0),
    ...Array.from({length: 10}, (_, i) => i / 10),
  ];
  jest.spyOn(Math, 'random').mockImplementation(() => values[i++]);
  expect(newAppScriptName(appID)).toBe('aback-broad-cockatoo-14pc0mi');
});
