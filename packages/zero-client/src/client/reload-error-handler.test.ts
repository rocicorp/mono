import {LogContext} from '@rocicorp/logger';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type Mock,
} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {
  FALLBACK_RELOAD_INTERVAL_MS,
  MAX_RELOAD_INTERVAL_MS,
  MIN_RELOAD_INTERVAL_MS,
  RELOAD_BACKOFF_STATE_KEY,
  reloadWithReason,
  reportReloadReason,
  resetBackoff,
  type BackoffState,
} from './reload-error-handler.ts';
import {storageMock} from './test-utils.ts';

describe('reloadWithReason', () => {
  let sessionStorageDescriptor: PropertyDescriptor;
  let sink: TestLogSink = new TestLogSink();
  let lc: LogContext;
  let storage: Record<string, string>;
  let reload: Mock<() => void>;
  const now = 12300000;

  beforeEach(() => {
    sessionStorageDescriptor = Object.getOwnPropertyDescriptor(
      globalThis,
      'sessionStorage',
    )!;

    vi.useFakeTimers({now});

    sink = new TestLogSink();
    lc = new LogContext('debug', {foo: 'bar'}, sink);
    reload = vi.fn();

    storage = {};
    vi.spyOn(globalThis, 'sessionStorage', 'get').mockImplementation(() =>
      storageMock(storage),
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();

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

    expect(reload).not.toBeCalled();
    vi.advanceTimersByTime(0);
    expect(reload).toHaveBeenCalledOnce();

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
      expect(reload).not.toHaveBeenCalled();
      vi.runOnlyPendingTimers();
      expect(reload).toHaveBeenCalledOnce();

      vi.runAllTimers();
    },
  );

  test('reloadWithReason no sessionStorage', () => {
    // @ts-expect-error This isa test so we do not play along with TS
    delete globalThis.sessionStorage;

    const sink = new TestLogSink();
    const lc = new LogContext('debug', {foo: 'bar'}, sink);

    const reload = vi.fn();
    reloadWithReason(lc, reload, 'my reason');

    expect(reload).not.toHaveBeenCalled();
    vi.advanceTimersByTime(FALLBACK_RELOAD_INTERVAL_MS);
    expect(reload).toHaveBeenCalledOnce();

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
            "in 10 seconds",
          ],
        ],
      ]
    `);
  });
});
