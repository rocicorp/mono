import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';
import {
  DID_NOT_CONNECT_VALUE,
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

    expect(reporter.calledOnceWithExactly(c.expected), c.name).true;

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

  mm.timeToConnectMs.set(100);
  mm.lastConnectError.set('bonk');

  await clock.tickAsync(REPORT_INTERVAL_MS);

  expect(
    reporter.calledOnceWithExactly([
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
    ]),
  ).true;

  reporter.resetHistory();
  mm.stop();

  await clock.tickAsync(REPORT_INTERVAL_MS * 2);
  expect(reporter.notCalled);
});
