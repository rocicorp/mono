import {describe, expect, test} from '@jest/globals';
import sizeof from 'firestore-size';
import {
  monthMetricsPath,
  splitDate,
  totalMetricsPath,
  yearMonth,
  type DayMetrics,
  type DayOfMonth,
  type Hour,
  type Metrics,
  type MonthMetrics,
} from './metrics.js';

describe('metrics schema', () => {
  const TEAM = '198SeL9eaaF';
  const APP = 'lm9glnnf';

  function emptyMonth(): MonthMetrics {
    return {
      teamID: TEAM,
      appID: APP,
      yearMonth: 202311,
      total: {},
      day: {},
    };
  }

  function fillMonth(values: Metrics): MonthMetrics {
    const month = emptyMonth();
    month.total = values;
    for (let d = 1; d <= 31; d++) {
      const day = d.toString() as DayOfMonth;
      const dayMetrics: DayMetrics = {
        total: values,
        hour: {},
      };
      for (let h = 0; h < 24; h++) {
        const hour = h.toString() as Hour;
        dayMetrics.hour[hour] = values;
      }
      month.day[day] = dayMetrics;
    }
    return month;
  }

  test('yearmonth', () => {
    expect(yearMonth(new Date(Date.UTC(2022, 0, 1)))).toBe(202201);
    expect(yearMonth(new Date(Date.UTC(2023, 8, 30)))).toBe(202309);
    expect(yearMonth(new Date(Date.UTC(2023, 11, 31)))).toBe(202312);
  });

  test('splitDate', () => {
    expect(splitDate(new Date(Date.UTC(2022, 0, 1, 2)))).toEqual([
      '2022',
      '1',
      '1',
      '2',
    ]);
    expect(splitDate(new Date(Date.UTC(2023, 8, 30, 3)))).toEqual([
      '2023',
      '9',
      '30',
      '3',
    ]);
    expect(splitDate(new Date(Date.UTC(2023, 11, 31, 15)))).toEqual([
      '2023',
      '12',
      '31',
      '15',
    ]);
  });

  test('empty document size', () => {
    expect(sizeof(emptyMonth())).toBe(94);
  });

  test('document size with two metrics', () => {
    const values: Metrics = {
      cs: 10000,
      cl: 10020,
    };
    const month = fillMonth(values);
    expect(sizeof(month)).toBe(19513);
  });

  test('document size with 100 metrics', () => {
    const values: {[key: string]: number} = {};
    for (let i = 0; i < 100; i++) {
      values[i < 10 ? `0${i}` : `${i}`] = 10000;
    }
    const month = fillMonth(values as Metrics);
    expect(sizeof(month)).toBe(856041);
    // 100 metrics is still within the 1MB doc size limit
    expect(
      sizeof(month) + monthMetricsPath('2023', '11', TEAM, APP).length,
    ).toBeLessThan(1024 * 1024);
  });

  test('document paths', () => {
    expect(monthMetricsPath('2023', '9', TEAM, APP)).toBe(
      `apps/${APP}/metrics/202309-${TEAM}`,
    );
    expect(monthMetricsPath('2023', '10', TEAM, APP)).toBe(
      `apps/${APP}/metrics/202310-${TEAM}`,
    );
    expect(monthMetricsPath('2023', '9', TEAM)).toBe(
      `teams/${TEAM}/metrics/202309`,
    );
    expect(monthMetricsPath('2023', '10', TEAM)).toBe(
      `teams/${TEAM}/metrics/202310`,
    );
    expect(monthMetricsPath('2023', '9', TEAM, null)).toBe(
      `teams/${TEAM}/metrics/202309`,
    );
    expect(monthMetricsPath('2023', '10', TEAM, null)).toBe(
      `teams/${TEAM}/metrics/202310`,
    );
    expect(totalMetricsPath(TEAM, APP)).toBe(
      `apps/${APP}/metrics/total-${TEAM}`,
    );
    expect(totalMetricsPath(TEAM)).toBe(`teams/${TEAM}/metrics/total`);
    expect(totalMetricsPath(TEAM, null)).toBe(`teams/${TEAM}/metrics/total`);
  });
});
