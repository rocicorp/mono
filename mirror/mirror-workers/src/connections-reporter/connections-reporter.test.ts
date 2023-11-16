import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {
  CONNECTION_SECONDS_CHANNEL_NAME,
  CONNECTION_SECONDS_V1_CHANNEL_NAME,
} from 'shared/src/events/connection-seconds.js';
import type {Env} from './index.js';
import reporter, {AUTH_DATA_HEADER_NAME} from './index.js';

describe('connections reporter', () => {
  const runningConnectionSecondsDS = {
    writeDataPoint: jest.fn(),
  };
  const connectionLifetimesDS = {
    writeDataPoint: jest.fn(),
  };

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  function env(): Env {
    return {runningConnectionSecondsDS, connectionLifetimesDS};
  }

  test('reports valid running connection seconds', () => {
    reporter.tail(
      [
        {
          event: null,
          eventTimestamp: null,
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
                period: 60,
                roomID: 'foo-room',
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: -40.2, // Should be ignored
                period: 60,
                roomID: 'bar-room',
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
                period: 60,
                roomID: 'baz-room',
              },
              timestamp: 0,
            },
          ],
        },
        {
          event: null,
          eventTimestamp: null,
          scriptTags: ['missing:tags'],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 0.5,
                period: 60,
                roomID: 'bonk-room',
              },
              timestamp: 0,
            },
          ],
        },
        {
          event: null,
          eventTimestamp: null,
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
                period: 60,
                roomID: 'boom-room',
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_CHANNEL_NAME,
              message: {
                elapsed: 33.5,
                period: 60,
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
                period: 30,
                roomID: 'vroom-vroom',
              },
              timestamp: 0,
            },
          ],
        },
      ],
      env(),
    );
    expect(runningConnectionSecondsDS.writeDataPoint).toBeCalledTimes(4);
    expect(
      runningConnectionSecondsDS.writeDataPoint.mock.calls.map(call => call[0]),
    ).toEqual([
      {blobs: ['baz', 'foo', 'foo-room'], doubles: [48.5, 60]},
      {blobs: ['baz', 'foo', 'baz-room'], doubles: [40.2, 60]},
      {blobs: ['faz', 'boo', 'boom-room'], doubles: [31.5, 60]},
      {blobs: ['faz', 'boo', 'vroom-vroom'], doubles: [15.2, 30]},
    ]);
  });

  test('reports valid v1 running connection seconds', () => {
    reporter.tail(
      [
        {
          event: null,
          eventTimestamp: null,
          scriptTags: [
            'appID:foo',
            'appName:bar',
            'teamID:baz',
            'teamLabel:bonk',
          ],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
              message: {
                elapsed: 48.5,
                interval: 60,
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
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
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
              message: {
                elapsed: 40.2,
                interval: 60,
              },
              timestamp: 0,
            },
          ],
        },
        {
          event: null,
          eventTimestamp: null,
          scriptTags: ['missing:tags'],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
              message: {
                elapsed: 0.5,
                interval: 60,
              },
              timestamp: 0,
            },
          ],
        },
        {
          event: null,
          eventTimestamp: null,
          scriptTags: [
            'appID:boo',
            'appName:far',
            'teamID:faz',
            'teamLabel:funk',
          ],
          diagnosticsChannelEvents: [
            {
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
              message: {
                elapsed: 31.5,
                interval: 60,
              },
              timestamp: 0,
            },
            {
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
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
              channel: CONNECTION_SECONDS_V1_CHANNEL_NAME,
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
    expect(runningConnectionSecondsDS.writeDataPoint).toBeCalledTimes(4);
    expect(
      runningConnectionSecondsDS.writeDataPoint.mock.calls.map(call => call[0]),
    ).toEqual([
      {blobs: ['baz', 'foo', ''], doubles: [48.5, 60]},
      {blobs: ['baz', 'foo', ''], doubles: [40.2, 60]},
      {blobs: ['faz', 'boo', ''], doubles: [31.5, 60]},
      {blobs: ['faz', 'boo', ''], doubles: [15.2, 30]},
    ]);
  });

  test('reports RoomDO connection lifetimes', () => {
    const TAIL_EVENT_TIME = 123456;
    jest.setSystemTime(TAIL_EVENT_TIME);

    reporter.tail(
      [
        {
          event: {
            request: {
              headers: {
                [AUTH_DATA_HEADER_NAME]: 'REDACTED',
              },
            },
          },
          eventTimestamp: 98765,
          scriptTags: [
            'appID:foo',
            'appName:bar',
            'teamID:baz',
            'teamLabel:bonk',
          ],
          diagnosticsChannelEvents: [],
        },
        {
          event: {
            request: {
              headers: {
                // Not from RoomDO
              },
            },
          },
          eventTimestamp: 98765,
          scriptTags: [
            'appID:foo',
            'appName:bar',
            'teamID:baz',
            'teamLabel:bonk',
          ],
          diagnosticsChannelEvents: [],
        },
        {
          event: {
            request: {
              headers: {
                [AUTH_DATA_HEADER_NAME]: 'REDACTED',
              },
            },
          },
          eventTimestamp: null,
          scriptTags: ['missing:tags'],
          diagnosticsChannelEvents: [],
        },
        {
          event: {
            /* not a fetch event */
          },
          eventTimestamp: 87328,
          scriptTags: [
            'appID:boo',
            'appName:far',
            'teamID:faz',
            'teamLabel:funk',
          ],
          diagnosticsChannelEvents: [],
        },
        {
          event: {
            request: {
              headers: {
                [AUTH_DATA_HEADER_NAME]: 'REDACTED',
              },
            },
          },
          eventTimestamp: 87328,
          scriptTags: [
            'appID:boo',
            'appName:far',
            'teamID:faz',
            'teamLabel:funk',
          ],
          diagnosticsChannelEvents: [],
        },
      ],
      env(),
    );
    expect(connectionLifetimesDS.writeDataPoint).toBeCalledTimes(2);
    expect(
      connectionLifetimesDS.writeDataPoint.mock.calls.map(call => call[0]),
    ).toEqual([
      {blobs: ['baz', 'foo'], doubles: [98765, TAIL_EVENT_TIME]},
      {blobs: ['faz', 'boo'], doubles: [87328, TAIL_EVENT_TIME]},
    ]);
  });
});
