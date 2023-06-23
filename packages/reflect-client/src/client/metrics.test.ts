import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import sinon from 'sinon';
import {
  DisconnectReason,
  Gauge,
  MetricManager,
  Point,
  REPORT_INTERVAL_MS,
  Series,
  State,
  TIME_TO_CONNECT_SPECIAL_VALUES,
} from './metrics.js';

teardown(() => {
  sinon.restore();
});

test('Gauge', () => {
  type Case = {
    name: string;
    value: number | undefined;
    time: number;
    expected: Point[];
  };

  const cases: Case[] = [
    {
      name: 'undefined',
      value: undefined,
      time: 100 * 1000,
      expected: [],
    },
    {
      name: 'val-10',
      value: 10,
      time: 200 * 1000,
      expected: [[200, [10]]],
    },
    {
      name: 'val-20',
      value: 20,
      time: 500 * 1000,
      expected: [[500, [20]]],
    },
  ];

  const g = new Gauge('mygauge');
  const clock = sinon.useFakeTimers();

  for (const c of cases) {
    clock.setSystemTime(c.time);
    if (c.value !== undefined) {
      g.set(c.value);
    }
    const series = g.flush();
    expect(series, c.name).deep.equal({metric: 'mygauge', points: c.expected});
  }
});

test('State', () => {
  type Case = {
    name: string;
    state: string | undefined;
    time: number;
    expected: ReturnType<State['flush']>;
  };

  const cases: Case[] = [
    {
      name: 'undefined',
      state: undefined,
      time: 100 * 1000,
      expected: undefined,
    },
    {
      name: 'state-foo',
      state: 'foo',
      time: 200 * 1000,
      expected: {
        metric: 'mygauge_foo',
        points: [[200, [1]]],
      },
    },
    {
      name: 'state-bar',
      state: 'bar',
      time: 500 * 1000,
      expected: {
        metric: 'mygauge_bar',
        points: [[500, [1]]],
      },
    },
  ];

  const clock = sinon.useFakeTimers();

  for (const c of cases) {
    clock.setSystemTime(c.time);

    const m1 = new State('mygauge');
    if (c.state !== undefined) {
      m1.set(c.state);
    }
    const s1 = m1.flush();
    expect(s1, c.name).deep.equal(c.expected);
    const s2 = m1.flush();
    expect(s2, c.name).deep.equal(c.expected);

    const m2 = new State('mygauge', true);
    if (c.state !== undefined) {
      m2.set(c.state);
    }
    const s3 = m2.flush();
    expect(s3, c.name).deep.equal(c.expected);
    const s4 = m2.flush();
    expect(s4, c.name).deep.equal(undefined);
  }
});

test('MetricManager', async () => {
  const clock = sinon.useFakeTimers();

  const reporter = sinon.mock().returns(Promise.resolve());
  const mm = new MetricManager({
    reportIntervalMs: REPORT_INTERVAL_MS,
    host: 'test-host',
    source: 'test-source',
    reporter,
    lc: Promise.resolve(new LogContext()),
  });

  type Case = {
    name: string;
    reportMetrics?: (metricsManager: MetricManager) => void;
    timeToConnect?: number;
    connectError?: DisconnectReason;
    extraTags?: string[];
    expected: Series[];
  };

  const cases: Case[] = [
    {
      name: 'no metrics',
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              REPORT_INTERVAL_MS / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.initialValue],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-1',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(2);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 2) / 1000, [2]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-2',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(1);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 3) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce client AbruptClose',
      reportMetrics: metricsManager => {
        metricsManager.setConnectError({client: 'AbruptClose'});
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 4) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_client_abrupt_close',
          points: [[(REPORT_INTERVAL_MS * 4) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce server Unauthorized',
      reportMetrics: metricsManager => {
        metricsManager.setConnectError({server: 'Unauthorized'});
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 5) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 5) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce-unchanged',
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 6) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 6) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'extra-tags',
      extraTags: ['foo:bar', 'hotdog'],
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 7) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source', 'foo:bar', 'hotdog'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 7) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source', 'foo:bar', 'hotdog'],
        },
      ],
    },
    {
      name: 'connected after error',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(5000);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 8) / 1000, [5000]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'error client ConnectTimeout',
      reportMetrics: metricsManager => {
        metricsManager.setConnectError({client: 'ConnectTimeout'});
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 9) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_client_connect_timeout',
          points: [[(REPORT_INTERVAL_MS * 9) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'setDisconnectedWaitingForVisible',
      reportMetrics: metricsManager => {
        metricsManager.setDisconnectedWaitingForVisible();
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [
            [
              (REPORT_INTERVAL_MS * 10) / 1000,
              [TIME_TO_CONNECT_SPECIAL_VALUES.disconnectedWaitingForVisible],
            ],
          ],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
  ];

  for (const c of cases) {
    if (c.reportMetrics) {
      c.reportMetrics(mm);
    }
    if (c.extraTags !== undefined) {
      mm.tags.push(...c.extraTags);
    }

    await clock.tickAsync(REPORT_INTERVAL_MS);

    expect(reporter.callCount).equals(1);
    expect(reporter.getCalls()[0].args[0]).to.deep.equal(c.expected);

    mm.tags.length = 1;

    reporter.resetHistory();
  }
});

test('MetricManager.stop', async () => {
  const clock = sinon.useFakeTimers();

  const reporter = sinon.mock().returns(Promise.resolve());
  const mm = new MetricManager({
    reportIntervalMs: REPORT_INTERVAL_MS,
    host: 'test-host',
    source: 'test-source',
    reporter,
    lc: Promise.resolve(new LogContext()),
  });

  mm.setConnectError({client: 'AbruptClose'});

  await clock.tickAsync(REPORT_INTERVAL_MS);

  expect(
    reporter.calledOnceWithExactly([
      {
        metric: 'time_to_connect_ms_v2',
        points: [
          [
            REPORT_INTERVAL_MS / 1000,
            [TIME_TO_CONNECT_SPECIAL_VALUES.connectError],
          ],
        ],
        host: 'test-host',
        tags: ['source:test-source'],
      },
      {
        metric: 'last_connect_error_v2_client_abrupt_close',
        points: [[REPORT_INTERVAL_MS / 1000, [1]]],
        host: 'test-host',
        tags: ['source:test-source'],
      },
    ]),
  ).true;

  reporter.resetHistory();
  mm.stop();

  await clock.tickAsync(REPORT_INTERVAL_MS * 2);
  expect(reporter.notCalled);
});
