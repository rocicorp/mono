import {describe, expect, test} from '@jest/globals';
import {daysInMonth, durationString} from './usage.js';

describe('duration string', () => {
  type Case = {
    name: string;
    duration: number;
    result: string;
  };
  const cases: Case[] = [
    {
      name: 'less than 10 seconds',
      duration: 8.9238,
      result: '0:09',
    },
    {
      name: 'more than 10 seconds',
      duration: 18.9238,
      result: '0:19',
    },
    {
      name: 'more than one minute, seconds padding',
      duration: 60 + 8.9238,
      result: '1:09',
    },
    {
      name: 'more than one minute, no seconds padding',
      duration: 60 + 18.9238,
      result: '1:19',
    },
    {
      name: 'more than 10 minutes, seconds padding',
      duration: 600 + 8.9238,
      result: '10:09',
    },
    {
      name: 'more than 10 minutes, no seconds padding',
      duration: 600 + 18.9238,
      result: '10:19',
    },
    {
      name: 'more than 1 hour, no minutes, seconds padding',
      duration: 3600 + 8.9238,
      result: '1:00:09',
    },
    {
      name: 'more than 1 hour, minutes, seconds padding',
      duration: 3600 + 60 + 8.9238,
      result: '1:01:09',
    },
    {
      name: 'more than 1 hour, no minutes or seconds padding',
      duration: 3600 + 600 + 18.9238,
      result: '1:10:19',
    },
    {
      name: 'more than 24 hours, no minute or seconds padding',
      duration: 25 * 3600 + 600 + 18.9238,
      result: '25:10:19',
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(durationString(c.duration)).toBe(c.result);
    });
  }
});

describe('days in month', () => {
  type Case = {
    name: string;
    date: number;
    days: number;
  };

  const cases: Case[] = [
    {
      name: 'Jan',
      date: Date.UTC(2023, 0),
      days: 31,
    },
    {
      name: 'Feb',
      date: Date.UTC(2023, 1),
      days: 28,
    },
    {
      name: 'Mar',
      date: Date.UTC(2023, 2),
      days: 31,
    },
    {
      name: 'Apr',
      date: Date.UTC(2023, 3),
      days: 30,
    },
    {
      name: 'May',
      date: Date.UTC(2023, 4),
      days: 31,
    },
    {
      name: 'Jun',
      date: Date.UTC(2023, 5),
      days: 30,
    },
    {
      name: 'Jul',
      date: Date.UTC(2023, 6),
      days: 31,
    },
    {
      name: 'Aug',
      date: Date.UTC(2023, 7),
      days: 31,
    },
    {
      name: 'Sep',
      date: Date.UTC(2023, 8),
      days: 30,
    },
    {
      name: 'Oct',
      date: Date.UTC(2023, 9),
      days: 31,
    },
    {
      name: 'Nov',
      date: Date.UTC(2023, 10),
      days: 30,
    },
    {
      name: 'Dec',
      date: Date.UTC(2023, 11),
      days: 31,
    },
    {
      name: 'Leap Year Feb',
      date: Date.UTC(2024, 1),
      days: 29,
    },
  ];

  for (const c of cases) {
    expect(daysInMonth(new Date(c.date))).toBe(c.days);
  }
});
