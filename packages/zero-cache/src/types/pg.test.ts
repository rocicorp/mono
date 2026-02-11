import {describe, expect, test} from 'vitest';
import {
  millisecondsToPostgresTime,
  postgresTimeToMilliseconds,
  timestampToFpMillis,
} from './pg.ts';

describe('timestampToFpMillis', () => {
  test.each([
    ['2019-01-11 22:30:35.381101-01', 1547249435381.101],
    ['2019-01-11 23:30:35.381101+00', 1547249435381.101],
    ['2019-01-12 00:30:35.381101+01', 1547249435381.101],

    ['2019-01-11 23:30:35.381101+01:01', 1547245775381.101],
    ['2019-01-11 22:30:35.381101+00:03', 1547245655381.101],

    ['2004-10-19 10:23:54.654321', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00:00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+02', 1098174234654.321],
    ['2024-12-05 16:38:21.907-05', 1733434701907],
    ['2024-12-05 16:38:21.907-05:30', 1733436501907],
  ])('parse timestamp: %s', (timestamp, result) => {
    // expect(new PreciseDate(timestamp).getTime()).toBe(Math.floor(result));
    expect(timestampToFpMillis(timestamp)).toBe(result);
  });
});

describe('millisecondsToPostgresTime', () => {
  describe('valid inputs', () => {
    test.each([
      ['0 milliseconds', 0, '00:00:00.000+00'],
      ['1 second (1000ms)', 1000, '00:00:01.000+00'],
      ['1 minute (60000ms)', 60000, '00:01:00.000+00'],
      ['1 hour (3600000ms)', 3600000, '01:00:00.000+00'],
      ['1 millisecond', 1, '00:00:00.001+00'],
      ['123 milliseconds', 123, '00:00:00.123+00'],
      ['999 milliseconds', 999, '00:00:00.999+00'],
      [
        'complex time 12:34:56.789',
        12 * 3600000 + 34 * 60000 + 56 * 1000 + 789,
        '12:34:56.789+00',
      ],
      [
        'maximum valid time (23:59:59.999)',
        24 * 60 * 60 * 1000 - 1,
        '23:59:59.999+00',
      ],
      [
        'single digit padding (01:02:03.004)',
        1 * 3600000 + 2 * 60000 + 3 * 1000 + 4,
        '01:02:03.004+00',
      ],
      ['millisecond padding - 1000ms', 1000, '00:00:01.000+00'],
      ['millisecond padding - 1010ms', 1010, '00:00:01.010+00'],
      ['millisecond padding - 1100ms', 1100, '00:00:01.100+00'],
    ])('should convert %s correctly to %s', (_caseName, input, expected) => {
      expect(millisecondsToPostgresTime(input)).toBe(expected);
    });
  });

  describe('edge cases', () => {
    test.each([
      [
        'last millisecond of day',
        23 * 3600000 + 59 * 60000 + 59 * 1000 + 999,
        '23:59:59.999+00',
      ],
      ['noon exactly', 12 * 3600000, '12:00:00.000+00'],
      ['10 seconds', 10000, '00:00:10.000+00'],
      ['100 seconds', 100000, '00:01:40.000+00'],
      ['1000 seconds', 1000000, '00:16:40.000+00'],
    ])('should handle %s', (_caseName, input, expected) => {
      expect(millisecondsToPostgresTime(input)).toBe(expected);
    });
  });

  describe('error handling', () => {
    test.each([
      ['negative milliseconds (-1)', -1],
      ['negative milliseconds (-1000)', -1000],
    ])('should throw error for %s', (_caseName, input) => {
      expect(() => millisecondsToPostgresTime(input)).toThrow(
        'Milliseconds cannot be negative',
      );
    });

    test.each([
      ['exactly 24 hours', 24 * 60 * 60 * 1000],
      ['24 hours + 1ms', 24 * 60 * 60 * 1000 + 1],
      ['100000000ms', 100000000],
    ])('should throw error for %s', (_caseName, input) => {
      expect(() => millisecondsToPostgresTime(input)).toThrow(
        'Milliseconds cannot exceed 24 hours (86400000ms)',
      );
    });
  });

  describe('precision and formatting', () => {
    test.each([
      ['zero', 0, '00:00:00.000+00'],
      ['1 second exact', 1000, '00:00:01.000+00'],
      ['1 second + 1ms', 1001, '00:00:01.001+00'],
      ['1 second + 10ms', 1010, '00:00:01.010+00'],
      ['1 second + 100ms', 1100, '00:00:01.100+00'],
    ])(
      'should include three decimal places for %s',
      (_caseName, input, expected) => {
        expect(millisecondsToPostgresTime(input)).toBe(expected);
      },
    );

    test.each([
      ['minimum value', 0],
      ['single millisecond', 1],
      ['max milliseconds only', 999],
      ['maximum value', 86399999],
    ])(
      'should maintain consistent format length for %s',
      (_caseName, input) => {
        const result = millisecondsToPostgresTime(input);
        expect(result.length).toBe(15); // HH:MM:SS.mmm+00 = 15 characters
        expect(result).toMatch(/^\d{2}:\d{2}:\d{2}\.\d{3}\+00$/);
      },
    );
  });

  describe('mathematical accuracy', () => {
    test.each([
      ['midnight', 0, 0, 0, 0, '00:00:00.000+00'],
      ['all ones', 1, 1, 1, 1, '01:01:01.001+00'],
      ['random time', 10, 30, 45, 500, '10:30:45.500+00'],
      ['end of day', 23, 59, 59, 999, '23:59:59.999+00'],
    ])(
      'should convert %s accurately',
      (_caseName, hours, minutes, seconds, ms, expected) => {
        const totalMs = hours * 3600000 + minutes * 60000 + seconds * 1000 + ms;
        expect(millisecondsToPostgresTime(totalMs)).toBe(expected);
      },
    );

    test.each([
      ['0.1ms rounds down', 0.1, '00:00:00.000+00'],
      ['999.9ms rounds down', 999.9, '00:00:00.999+00'],
      ['1000.1ms rounds down', 1000.1, '00:00:01.000+00'],
    ])(
      'should handle floating point precision: %s',
      (_caseName, input, expected) => {
        expect(millisecondsToPostgresTime(input)).toBe(expected);
      },
    );
  });
});

describe('postgresTimeToMilliseconds', () => {
  describe('valid time strings', () => {
    test.each([
      ['midnight', '00:00:00', 0],
      ['one second', '00:00:01', 1000],
      ['one minute', '00:01:00', 60000],
      ['one hour', '01:00:00', 3600000],
      ['noon', '12:00:00', 43200000],
      ['complex time', '12:34:56', 45296000],
      ['end of day minus 1 second', '23:59:59', 86399000],
      ['single digit hours', '9:30:45', 34245000],
      ['double digit hours', '09:30:45', 34245000],
      ['all components', '23:45:37', 85537000],
    ])('should parse %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });

  describe('time strings with milliseconds', () => {
    test.each([
      ['zero milliseconds explicit', '00:00:00.000', 0],
      ['1 millisecond', '00:00:00.001', 1],
      ['10 milliseconds', '00:00:00.010', 10],
      ['100 milliseconds', '00:00:00.100', 100],
      ['999 milliseconds', '00:00:00.999', 999],
      ['complex with milliseconds', '12:34:56.789', 45296789],
      ['end of day with milliseconds', '23:59:59.999', 86399999],
      ['mixed components', '01:02:03.456', 3723456],
    ])('should parse %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });

  describe('millisecond padding behavior', () => {
    test.each([
      ['single digit as hundreds', '12:34:56.7', 45296700],
      ['two digits as tens', '12:34:56.78', 45296780],
      ['three digits exact', '12:34:56.789', 45296789],
      ['trailing zeros implicit', '00:00:01.5', 1500],
      ['middle zero preserved', '00:00:01.05', 1050],
      ['leading zeros preserved', '00:00:01.005', 1005],
    ])('should handle %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });

  describe('microsecond precision truncation', () => {
    test.each([
      ['4 digits - truncate 1 microsecond digit', '12:34:56.7891', 45296789],
      ['5 digits - truncate 2 microsecond digits', '12:34:56.78912', 45296789],
      ['6 digits - full microseconds truncated', '12:34:56.789123', 45296789],
      ['microseconds round down case 1', '00:00:00.1234', 123],
      ['microseconds round down case 2', '00:00:00.1239', 123],
      ['microseconds round down case 3', '00:00:00.9999', 999],
      ['microseconds with zeros', '00:00:00.1000', 100],
      ['microseconds all dropped', '00:00:00.000001', 0],
      ['microseconds partially kept', '00:00:00.123456', 123],
      ['complex time with microseconds', '23:59:59.999999', 86399999],
    ])('should truncate microseconds: %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });

  describe('PostgreSQL 24:00:00 edge case', () => {
    test.each([
      ['24:00:00 exactly', '24:00:00', 86400000],
      ['24:00:00 with zero milliseconds', '24:00:00.000', 86400000],
      ['24:00:00 with zero microseconds', '24:00:00.000000', 86400000],
      ['24:00:00 with UTC timezone', '24:00:00+00', 86400000],
      ['24:00:00 with positive timezone offset', '24:00:00+02', 79200000],
      ['24:00:00 with negative timezone (wraps)', '24:00:00-05', 18000000],
    ])('should handle %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });

  describe('invalid time strings', () => {
    test.each([
      ['empty string', ''],
      ['null value', null],
      ['undefined value', undefined],
      ['number instead of string', 12345],
      ['object instead of string', {}],
      ['array instead of string', []],
    ])('should throw for %s', (_caseName, input) => {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      expect(() => postgresTimeToMilliseconds(input as any)).toThrow(
        'Invalid time string: must be a non-empty string',
      );
    });
  });

  describe('invalid time formats', () => {
    test.each([
      ['missing colons', '123456'],
      ['single colon only', '12:34'],
      ['extra colons', '12:34:56:78'],
      ['non-numeric hours', 'AB:34:56'],
      ['non-numeric minutes', '12:AB:56'],
      ['non-numeric seconds', '12:34:AB'],
      ['non-numeric milliseconds', '12:34:56.ABC'],
      ['invalid separator', '12-34-56'],
      ['space separator', '12 34 56'],
      ['comma decimal', '12:34:56,789'],
      ['missing leading zeros not allowed for minutes', '12:3:56'],
      ['missing leading zeros not allowed for seconds', '12:34:5'],
      ['too many hour digits', '123:34:56'],
      ['negative values', '-12:34:56'],
    ])('should throw for invalid format: %s', (_caseName, input) => {
      expect(() => postgresTimeToMilliseconds(input)).toThrow(
        /Invalid time format/,
      );
    });
  });

  describe('out of range values', () => {
    test.each([
      [
        'hours = 24 with minutes',
        '24:01:00',
        'Invalid time: when hours is 24, minutes, seconds, and milliseconds must be 0',
      ],
      [
        'hours = 24 with seconds',
        '24:00:01',
        'Invalid time: when hours is 24, minutes, seconds, and milliseconds must be 0',
      ],
      [
        'hours = 24 with milliseconds',
        '24:00:00.001',
        'Invalid time: when hours is 24, minutes, seconds, and milliseconds must be 0',
      ],
      [
        'hours > 24',
        '25:00:00',
        'Invalid hours: 25. Must be between 0 and 24 (24 means end of day)',
      ],
      [
        'hours = 99',
        '99:00:00',
        'Invalid hours: 99. Must be between 0 and 24 (24 means end of day)',
      ],
      [
        'minutes = 60',
        '12:60:00',
        'Invalid minutes: 60. Must be between 0 and 59',
      ],
      [
        'minutes = 99',
        '12:99:00',
        'Invalid minutes: 99. Must be between 0 and 59',
      ],
      [
        'seconds = 60',
        '12:34:60',
        'Invalid seconds: 60. Must be between 0 and 59',
      ],
      [
        'seconds = 99',
        '12:34:99',
        'Invalid seconds: 99. Must be between 0 and 59',
      ],
    ])('should throw for %s', (_caseName, input, expectedError) => {
      expect(() => postgresTimeToMilliseconds(input)).toThrow(expectedError);
    });
  });

  describe('round trip conversions', () => {
    test.each([
      ['midnight', 0],
      ['one millisecond', 1],
      ['one second', 1000],
      ['one minute', 60000],
      ['one hour', 3600000],
      ['complex time', 45296789],
      ['maximum milliseconds', 86399999],
    ])('should round trip: %s', (_caseName, milliseconds) => {
      const timeString = millisecondsToPostgresTime(milliseconds);
      const result = postgresTimeToMilliseconds(timeString);
      expect(result).toBe(milliseconds);
    });
  });

  describe('PostgreSQL compatibility', () => {
    test.each([
      ['standard format', '13:45:30', 49530000],
      ['with milliseconds', '13:45:30.123', 49530123],
      ['with microseconds (truncated)', '13:45:30.123456', 49530123],
      ['maximum precision', '23:59:59.999999', 86399999],
      ['single digit hour', '1:00:00', 3600000],
      ['double digit hour', '01:00:00', 3600000],
    ])('should handle PostgreSQL format: %s', (_caseName, input, expected) => {
      expect(postgresTimeToMilliseconds(input)).toBe(expected);
    });
  });
});
