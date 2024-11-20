import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.js';
import {
  FALLBACK_RELOAD_INTERVAL_MS,
  MAX_RELOAD_INTERVAL_MS,
  MIN_RELOAD_INTERVAL_MS,
  RELOAD_BACKOFF_STATE_KEY,
  reloadWithReason,
  reportReloadReason,
  resetBackoff,
  type BackoffState,
} from './reload-error-handler.js';

function storageMock(storage: Record<string, string>): Storage {
  return {
    setItem: (key, value) => {
      storage[key] = value || '';
    },
    getItem: key => (key in storage ? storage[key] : null),
    removeItem: key => {
      delete storage[key];
    },
    clear: () => {
      for (const key of Object.keys(storage)) {
        delete storage[key];
      }
    },
    get length() {
      return Object.keys(storage).length;
    },
    key: i => {
      const keys = Object.keys(storage);
      return keys[i] || null;
    },
  };
}

describe('reloadWithReason', () => {
  let sessionStorageDescriptor: PropertyDescriptor;
  let sink: TestLogSink = new TestLogSink();
  let lc: LogContext;
  let storage: Record<string, string>;
  let reload: sinon.SinonSpy;
  const now = 12300000;

  beforeEach(() => {
    sessionStorageDescriptor = Object.getOwnPropertyDescriptor(
      globalThis,
      'sessionStorage',
    )!;

    vi.useFakeTimers();
    vi.setSystemTime(now);

    sink = new TestLogSink();
    lc = new LogContext('debug', {foo: 'bar'}, sink);
    reload = sinon.fake();

    storage = {};
    sinon.replaceGetter(globalThis, 'sessionStorage', () =>
      storageMock(storage),
    );
  });

  afterEach(() => {
    vi.useRealTimers();
    sinon.restore();

    Object.defineProperty(
      globalThis,
      'sessionStorage',
      sessionStorageDescriptor,
    );
  });

  test('initial reloadWithReason', () => {
    storage['unrelated'] = 'foo';

    reloadWithReason(lc, reload, 'my reason');
    expect(storage).toMatchInlineSnapshot(`
    {
      "_zeroReloadBackoffState": "{"lastReloadTime":12300000,"nextIntervalMs":500}",
      "_zeroReloadReason": "my reason",
      "unrelated": "foo",
    }
  `);

    expect(reload.calledOnce).equal(false);
    vi.advanceTimersByTime(0);
    expect(reload.calledOnce).equal(true);

    expect(sink.messages[0]).toMatchInlineSnapshot(`
    [
      "error",
      {
        "foo": "bar",
      },
      [
        "my reason",
        "
    ",
        "reloading",
        "",
      ],
    ]
  `);
    reportReloadReason(lc);
    expect(sink.messages[1]).toMatchInlineSnapshot(`
    [
      "error",
      {
        "foo": "bar",
      },
      [
        "Zero reloaded the page.",
        "my reason",
      ],
    ]
  `);

    resetBackoff();
    expect(storage[RELOAD_BACKOFF_STATE_KEY]).toBeUndefined();
  });

  test.each([
    [
      'after reload',
      {lastReloadTime: now - 100, nextIntervalMs: 1000},
      {lastReloadTime: now + 900, nextIntervalMs: 2000},
    ],
    [
      'after manual reload before timer',
      {lastReloadTime: now + 100, nextIntervalMs: 1000},
      {lastReloadTime: now + 100, nextIntervalMs: 1000},
    ],
    [
      'max interval',
      {lastReloadTime: now - 40_000, nextIntervalMs: 32_000},
      {lastReloadTime: now, nextIntervalMs: MAX_RELOAD_INTERVAL_MS},
    ],
    [
      'restart after really old backoff',
      {lastReloadTime: now - 400_000, nextIntervalMs: MAX_RELOAD_INTERVAL_MS},
      {lastReloadTime: now, nextIntervalMs: MIN_RELOAD_INTERVAL_MS},
    ],
    [
      'unparsable backoff state',
      {oldBackoffStateProtocol: now - 400_000} as unknown as BackoffState,
      {lastReloadTime: now, nextIntervalMs: MIN_RELOAD_INTERVAL_MS},
    ],
  ] satisfies [name: string, last: BackoffState, next: BackoffState][])(
    'backoff: %s',
    (_, last, next) => {
      storage[RELOAD_BACKOFF_STATE_KEY] = JSON.stringify(last);
      reloadWithReason(lc, reload, 'my reason');
      expect(JSON.parse(storage[RELOAD_BACKOFF_STATE_KEY])).toEqual(next);

      // Subsequent calls should not change the timer or state.
      reloadWithReason(lc, reload, 'my reason');
      reloadWithReason(lc, reload, 'my reason');
      expect(JSON.parse(storage[RELOAD_BACKOFF_STATE_KEY])).toEqual(next);

      // Fire (and thus clear) the timer.
      expect(reload.calledOnce).equal(false);
      vi.advanceTimersToNextTimer();
      expect(reload.calledOnce).equal(true);
    },
  );

  test('reloadWithReason no sessionStorage', () => {
    // @ts-expect-error This isa test so we do not play along with TS
    delete globalThis.sessionStorage;

    const sink = new TestLogSink();
    const lc = new LogContext('debug', {foo: 'bar'}, sink);

    const reload = sinon.fake();
    reloadWithReason(lc, reload, 'my reason');

    expect(reload.calledOnce).equal(false);
    vi.advanceTimersByTime(FALLBACK_RELOAD_INTERVAL_MS);
    expect(reload.calledOnce).equal(true);

    expect(sink.messages).toMatchInlineSnapshot(`
      [
        [
          "warn",
          {
            "foo": "bar",
          },
          [
            "sessionStorage not supported. backing off in 10 seconds",
          ],
        ],
        [
          "error",
          {
            "foo": "bar",
          },
          [
            "my reason",
            "
      ",
            "reloading",
            " in 10 seconds",
          ],
        ],
      ]
    `);
  });
});
