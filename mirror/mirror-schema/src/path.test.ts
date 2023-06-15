import {describe, expect, test} from '@jest/globals';
import * as path from './path.js';

describe('firestore/path', () => {
  test('joins', () => {
    expect(path.join('foo', 'bar', 'baz')).toBe('foo/bar/baz');
    expect(path.join('path', 'with', 'a space')).toBe('path/with/a space');
    expect(path.join('starts', 'with', '.dot')).toBe('starts/with/.dot');
    expect(path.join('starts', 'with', '..two-dots')).toBe(
      'starts/with/..two-dots',
    );
  });

  test('appends', () => {
    expect(path.append('foo/bar', 'baz')).toBe('foo/bar/baz');
    expect(path.append('path/with', 'a space')).toBe('path/with/a space');
    expect(path.append('starts/with', '.dot')).toBe('starts/with/.dot');
    expect(path.append('starts/with', '..two-dots')).toBe(
      'starts/with/..two-dots',
    );
  });

  test('rejects invalid segments', () => {
    for (const invalidSegment of ['.', '..', '__no good__', 'no/good']) {
      expect(() => path.join(invalidSegment)).toThrow(
        path.InvalidPathSegmentError,
      );
      expect(() => path.append('path', 'with', invalidSegment)).toThrow(
        path.InvalidPathSegmentError,
      );
    }
  });

  test('rejects long paths', () => {
    // Just fits within the MAX of 1500
    const longSegment = 'a'.repeat(1495);
    path.join('path', longSegment);
    path.append('path', longSegment);

    // Too long
    const tooLongSegment = 'a'.repeat(1496);
    expect(() => path.join('path', tooLongSegment)).toThrow(
      path.InvalidPathLengthError,
    );
    expect(() => path.append('path', tooLongSegment)).toThrow(
      path.InvalidPathLengthError,
    );
  });
});
