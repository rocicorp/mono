import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import sinon from 'sinon';
import {
  DID_NOT_CONNECT_VALUE,
  DisconnectReason,
  Gauge,
  MetricManager,
  Point,
  REPORT_INTERVAL_MS,
  Series,
  State,
} from './metrics.js';

teardown(() => {
  sinon.restore();
});

test('Gauge', () => {
  type Case = {
    name: string;
    value: number | undefined;
    time: number;
    expected: Point[] | undefined;
  };

  const cases: Case[] = [
    {
      name: 'undefined',
      value: undefined,
      time: 100 * 1000,
      expected: undefined,
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
    {
      name: 'clear',
      value: undefined,
      time: 700 * 1000,
      expected: undefined,
    },
  ];

  const g = new Gauge('mygauge');
  const clock = sinon.useFakeTimers();

  for (const c of cases) {
    clock.setSystemTime(c.time);
    if (c.value !== undefined) {
      g.set(c.value);
    } else if (g.get() !== undefined) {
      g.clear();
    }
    const series = g.flush();
    if (c.expected === undefined) {
      expect(series, c.name).to.be.undefined;
    } else {
      expect(series, c.name).deep.equal({
        metric: 'mygauge',
        points: c.expected,
      });
    }
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

test('MetricManager v1 connect metrics', async () => {
  const clock = sinon.useFakeTimers();

  const reporter = sinon.mock().returns(Promise.resolve());
  const mm = new MetricManager({
    reportIntervalMs: REPORT_INTERVAL_MS,
    host: 'test-host',
    source: 'test-source',
    reporter,
    lc: new LogContext(),
  });

  type Case = {
    name: string;
    timeToConnect?: number | undefined;
    lastConnectError?: string | undefined;
    extraTags?: string[];
    expected: Series[];
  };

  const cases: Case[] = [
    {
      name: 'no metrics',
      expected: [
        {
          metric: 'time_to_connect_ms',
          points: [[REPORT_INTERVAL_MS / 1000, [DID_NOT_CONNECT_VALUE]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-1',
      timeToConnect: 2,
      expected: [
        {
          metric: 'time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 2) / 1000, [2]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-2',
      timeToConnect: 1,
      expected: [
        {
          metric: 'time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 3) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce-bonk',
      lastConnectError: 'bonk',
      expected: [
        {
          metric: 'time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 4) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_bonk',
          points: [[(REPORT_INTERVAL_MS * 4) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce-nuts',
      lastConnectError: 'nuts',
      expected: [
        {
          metric: 'time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 5) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_nuts',
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
          metric: 'time_to_connect_ms',
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
          metric: 'time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 7) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source', 'foo:bar', 'hotdog'],
        },
      ],
    },
  ];

  let intervalTickCount = 0;
  for (const c of cases) {
    if (c.timeToConnect !== undefined) {
      mm.timeToConnectMs.set(c.timeToConnect);
    }
    if (c.lastConnectError !== undefined) {
      mm.lastConnectError.set(c.lastConnectError);
    }
    if (c.extraTags !== undefined) {
      mm.tags.push(...c.extraTags);
    }

    await clock.tickAsync(REPORT_INTERVAL_MS);
    intervalTickCount++;

    expect(reporter.callCount).equals(1);
    expect(reporter.getCalls()[0].args[0]).to.deep.equal([
      ...c.expected,
      {
        host: 'test-host',
        metric: 'not_connected_init',
        points: [[(REPORT_INTERVAL_MS * intervalTickCount) / 1000, [1]]],
        tags: ['source:test-source', ...(c.extraTags ?? [])],
      },
    ]);

    mm.tags.length = 1;

    reporter.resetHistory();
  }
});

test('MetricManager v2 connect metrics', async () => {
  const clock = sinon.useFakeTimers();

  const reporter = sinon.mock().returns(Promise.resolve());
  const mm = new MetricManager({
    reportIntervalMs: REPORT_INTERVAL_MS,
    host: 'test-host',
    source: 'test-source',
    reporter,
    lc: new LogContext(),
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
          metric: 'not_connected_init',
          points: [[REPORT_INTERVAL_MS / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'setDisconnectedWaitingForVisible after initial',
      reportMetrics: metricsManager => {
        metricsManager.setDisconnectedWaitingForVisible();
      },
      expected: [
        {
          metric: 'not_connected_hidden_was_init',
          points: [[(REPORT_INTERVAL_MS * 2) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-1',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(2, 2);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 3) / 1000, [2]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'total_time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 3) / 1000, [2]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'ttc-2',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(1, 5);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 4) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'total_time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 4) / 1000, [5]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'setDisconnectedWaitingForVisible after connect',
      reportMetrics: metricsManager => {
        metricsManager.setDisconnectedWaitingForVisible();
      },
      expected: [
        {
          metric: 'not_connected_hidden',
          points: [[(REPORT_INTERVAL_MS * 5) / 1000, [1]]],
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
          metric: 'not_connected_error',
          points: [[(REPORT_INTERVAL_MS * 6) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_client_abrupt_close',
          points: [[(REPORT_INTERVAL_MS * 6) / 1000, [1]]],
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
          metric: 'not_connected_error',
          points: [[(REPORT_INTERVAL_MS * 7) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 7) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'lce-unchanged',
      expected: [
        {
          metric: 'not_connected_error',
          points: [[(REPORT_INTERVAL_MS * 8) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 8) / 1000, [1]]],
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
          metric: 'not_connected_error',
          points: [[(REPORT_INTERVAL_MS * 9) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source', 'foo:bar', 'hotdog'],
        },
        {
          metric: 'last_connect_error_v2_server_unauthorized',
          points: [[(REPORT_INTERVAL_MS * 9) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source', 'foo:bar', 'hotdog'],
        },
      ],
    },
    {
      name: 'connected after error',
      reportMetrics: metricsManager => {
        metricsManager.setConnected(5000, 8000);
      },
      expected: [
        {
          metric: 'time_to_connect_ms_v2',
          points: [[(REPORT_INTERVAL_MS * 10) / 1000, [5000]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'total_time_to_connect_ms',
          points: [[(REPORT_INTERVAL_MS * 10) / 1000, [8000]]],
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
          metric: 'not_connected_error',
          points: [[(REPORT_INTERVAL_MS * 11) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
        {
          metric: 'last_connect_error_v2_client_connect_timeout',
          points: [[(REPORT_INTERVAL_MS * 11) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
    {
      name: 'setDisconnectedWaitingForVisible after error',
      reportMetrics: metricsManager => {
        metricsManager.setDisconnectedWaitingForVisible();
      },
      expected: [
        {
          metric: 'not_connected_hidden_was_error',
          points: [[(REPORT_INTERVAL_MS * 12) / 1000, [1]]],
          host: 'test-host',
          tags: ['source:test-source'],
        },
      ],
    },
  ];

  let intervalTickCount = 0;
  for (const c of cases) {
    if (c.reportMetrics) {
      c.reportMetrics(mm);
    }
    if (c.extraTags !== undefined) {
      mm.tags.push(...c.extraTags);
    }

    await clock.tickAsync(REPORT_INTERVAL_MS);
    intervalTickCount++;

    expect(reporter.callCount, c.name).equals(1);
    expect(reporter.getCalls()[0].args[0], c.name).to.deep.equal([
      {
        host: 'test-host',
        metric: 'time_to_connect_ms',
        points: [
          [
            (REPORT_INTERVAL_MS * intervalTickCount) / 1000,
            [DID_NOT_CONNECT_VALUE],
          ],
        ],
        tags: ['source:test-source', ...(c.extraTags ?? [])],
      },
      ...c.expected,
    ]);

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
    lc: new LogContext(),
  });

  mm.timeToConnectMs.set(100);
  mm.lastConnectError.set('bonk');
  mm.setConnected(100, 100);

  await clock.tickAsync(REPORT_INTERVAL_MS);
  expect(reporter.callCount).equals(1);
  expect(reporter.getCalls()[0].args[0]).to.deep.equal([
    {
      metric: 'time_to_connect_ms',
      points: [[REPORT_INTERVAL_MS / 1000, [100]]],
      host: 'test-host',
      tags: ['source:test-source'],
    },
    {
      metric: 'last_connect_error_bonk',
      points: [[REPORT_INTERVAL_MS / 1000, [1]]],
      host: 'test-host',
      tags: ['source:test-source'],
    },
    {
      metric: 'time_to_connect_ms_v2',
      points: [[REPORT_INTERVAL_MS / 1000, [100]]],
      host: 'test-host',
      tags: ['source:test-source'],
    },
    {
      metric: 'total_time_to_connect_ms',
      points: [[REPORT_INTERVAL_MS / 1000, [100]]],
      host: 'test-host',
      tags: ['source:test-source'],
    },
  ]);

  reporter.resetHistory();
  mm.stop();

  await clock.tickAsync(REPORT_INTERVAL_MS * 2);
  expect(reporter.notCalled);
});
