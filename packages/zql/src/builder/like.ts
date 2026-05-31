import {assertString} from 'shared/src/asserts.ts';
import type {NonNullValue, SimplePredicateNoNull} from './filter.ts';

export function getLikePredicate(
  pattern: NonNullValue,
  flags: 'i' | '',
): SimplePredicateNoNull {
  const op = getLikeOp(String(pattern), flags);
  return (lhs: NonNullValue) => {
    assertString(lhs);
    return op(String(lhs));
  };
}

const likePatternRe = /_|%|\\/;

function getLikeOp(pattern: string, flags: 'i' | ''): (lhs: string) => boolean {
  // if lhs does not contain '%' or '_' then it is a simple string comparison.
  // if it does contain '%' or '_' then it is a regex comparison.
  // '%' is a wildcard for any number of characters
  // '_' is a wildcard for a single character
  // Postgres SQL allows escaping using `\`.

  if (!likePatternRe.test(pattern)) {
    if (flags === 'i') {
      const rhsLower = pattern.toLowerCase();
      return (lhs: string) => lhs.toLowerCase() === rhsLower;
    }
    return (lhs: string) => lhs === pattern;
  }
  const re = patternToRegExp(pattern, flags);
  return (lhs: string) => re.test(lhs);
}

const specialCharsRe = /[$()*+.?[\]\\^{|}]/;

function patternToRegExp(source: string, flags: '' | 'i' = ''): RegExp {
  // There are a few cases:
  // % => .*
  // _ => .
  // \x => \x for any x except special regexp chars
  // special regexp chars => \special regexp chars
  let pattern = '^';
  for (let i = 0; i < source.length; i++) {
    let c = source[i];
    switch (c) {
      case '%':
        pattern += '.*';
        break;
      case '_':
        pattern += '.';
        break;

      // @ts-expect-error fallthrough
      case '\\':
        if (i === source.length - 1) {
          throw new Error('LIKE pattern must not end with escape character');
        }
        i++;
        c = source[i];

      // fall through
      default:
        if (specialCharsRe.test(c)) {
          pattern += '\\';
        }
        pattern += c;

        break;
    }
  }
  // Use the `s` (dotall) flag so `.` (from `_`) and `.*` (from `%`) match
  // newlines, and keep `^`/`$` anchored to the whole string. The `m` (multiline)
  // flag was wrong on both counts: it let `^`/`$` match interior line boundaries
  // (false positives, e.g. 'fooa\nbar' LIKE 'foo_') while the wildcards still
  // skipped newlines (false negatives, e.g. 'a\nb' NOT LIKE 'a%b').
  return new RegExp(pattern + '$', flags + 's');
}
