import {afterEach, describe, expect, jest, test} from '@jest/globals';
import {CONNECTION_SECONDS_CHANNEL_NAME} from 'shared/src/events/connection-seconds.js';
import type {Env} from './index.js';
import reporter from './index.js';

describe('connections reporter', () => {
  const dataset = {
    writeDataPoint: jest.fn(),
  };

  afterEach(() => {
    jest.resetAllMocks();
  });

  function env(): Env {
    return {runningConnectionSecondsDS: dataset};
  }

  test('reports valid data points', () => {
    reporter.tail(
      [
        {
          scriptTags: [
            'appID:foo',
            'appName:bar',
            'teamID:baz',
            'teamLabel:bonk',
          ],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 48.5,
                interval: 60,
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: -40.2, // Should be ignored
                interval: 60,
              },
              timestamp: 0,
            },
            {
              channel: 'unrelated channel',
              message: {
                foo: 'bar',
                baz: 'bonk',
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 40.2,
                interval: 60,
              },
              timestamp: 0,
            },
          ],
        },
        {
          scriptTags: ['missing:tags'],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 0.5,
                interval: 60,
              },
              timestamp: 0,
            },
          ],
        },
        {
          scriptTags: [
            'appID:boo',
            'appName:far',
            'teamID:faz',
            'teamLabel:funk',
          ],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 31.5,
                interval: 60,
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 33.5,
                interval: 60,
                malformed: 'message should be ignored',
              },
              timestamp: 0,
            },
            {
              channel: 'unrelated channel',
              message: {
                foo: 'bar',
                baz: 'bonk',
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 15.2,
                interval: 30,
              },
              timestamp: 0,
            },
          ],
        },
      ],
      env(),
    );
    expect(dataset.writeDataPoint).toBeCalledTimes(4);
    expect(dataset.writeDataPoint.mock.calls.map(call => call[0])).toEqual([
      {blobs: ['baz', 'foo'], doubles: [48.5, 60]},
      {blobs: ['baz', 'foo'], doubles: [40.2, 60]},
      {blobs: ['faz', 'boo'], doubles: [31.5, 60]},
      {blobs: ['faz', 'boo'], doubles: [15.2, 30]},
    ]);
  });
});
