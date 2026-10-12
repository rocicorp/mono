import {describe, expect, test} from 'vitest';
import {
  hasSelectStar,
  shouldCheck,
  stripCommentOnlyLines,
  toPosixPath,
} from './index.js';

// These tests are deliberately platform-independent: they feed Windows-shaped
// input explicitly rather than depending on the host separator, so a regression
// is caught on a POSIX CI runner too. That matters because every bug fixed here
// is invisible on POSIX by construction.

describe('toPosixPath', () => {
  test('rewrites Windows separators', () => {
    expect(toPosixPath('C:\\repo\\packages\\zero-cache\\src\\db.ts')).toBe(
      'C:/repo/packages/zero-cache/src/db.ts',
    );
  });

  test('leaves a POSIX path untouched', () => {
    expect(toPosixPath('/repo/packages/zero-cache/src/db.ts')).toBe(
      '/repo/packages/zero-cache/src/db.ts',
    );
  });
});

describe('shouldCheck', () => {
  const include = ['packages/zero-cache/src/'];

  test('matches a Windows path against a POSIX include prefix', () => {
    // Before normalization this returned false, so no-select-star silently did
    // not run on Windows at all.
    expect(
      shouldCheck('C:\\repo\\packages\\zero-cache\\src\\query.ts', include),
    ).toBe(true);
  });

  test('matches a POSIX path against a POSIX include prefix', () => {
    expect(shouldCheck('/repo/packages/zero-cache/src/query.ts', include)).toBe(
      true,
    );
  });

  test('tolerates a Windows-shaped include prefix', () => {
    expect(
      shouldCheck('/repo/packages/zero-cache/src/query.ts', [
        'packages\\zero-cache\\src\\',
      ]),
    ).toBe(true);
  });

  test('skips a file outside the include prefixes', () => {
    expect(shouldCheck('C:\\repo\\packages\\zql\\src\\query.ts', include)).toBe(
      false,
    );
  });

  test('skips test files on both separators', () => {
    expect(
      shouldCheck('C:\\repo\\packages\\zero-cache\\src\\a.test.ts', include),
    ).toBe(false);
    expect(
      shouldCheck('/repo/packages/zero-cache/src/a.test.ts', include),
    ).toBe(false);
  });

  test('skips test directories on both separators', () => {
    // This exclusion was also dead on Windows: '/test/' never matched a
    // backslash path, so test-directory files were linted there but not on CI.
    expect(
      shouldCheck('C:\\repo\\packages\\zero-cache\\src\\test\\a.ts', include),
    ).toBe(false);
    expect(
      shouldCheck('/repo/packages/zero-cache/src/test/a.ts', include),
    ).toBe(false);
  });

  test('skips snapshots and generated files on both separators', () => {
    expect(
      shouldCheck(
        'C:\\repo\\packages\\zero-cache\\src\\__snapshots__\\a.ts',
        include,
      ),
    ).toBe(false);
    expect(
      shouldCheck(
        'C:\\repo\\packages\\zero-cache\\src\\a_generated.ts',
        include,
      ),
    ).toBe(false);
  });

  test('an empty include list never matches', () => {
    expect(shouldCheck('C:\\repo\\packages\\zero-cache\\src\\a.ts', [])).toBe(
      false,
    );
  });
});

// `no-select-star`'s two halves. Exported and, until now, covered only through
// the rule — so a change to either was visible to nothing but a smoke fixture
// that happens to exercise the one path it takes.

test('a comment-only line cannot carry a SELECT * into the match', () => {
  // The point of stripping: a rule that read commented-out SQL would fire on
  // prose, and the one that matters is a `--` line, which is SQL's own comment
  // inside a template literal.
  expect(hasSelectStar(stripCommentOnlyLines('-- SELECT * FROM t'))).toBe(
    false,
  );
  expect(hasSelectStar(stripCommentOnlyLines('   // SELECT * FROM t'))).toBe(
    false,
  );
  // Only comment-ONLY lines go. A trailing comment leaves the statement intact,
  // which is deliberate: the code before it still runs.
  expect(hasSelectStar(stripCommentOnlyLines('SELECT * FROM t -- why'))).toBe(
    true,
  );
  // Line-preserving, so a reported line number still points at the right line.
  expect(stripCommentOnlyLines('a\n// b\nc')).toBe('a\n\nc');
});

test('select-star matching is about the projection, not the word', () => {
  for (const yes of [
    'SELECT * FROM t',
    'select * from t',
    'SELECT *, id FROM t',
    'SELECT\n  *\nFROM t',
    'select a.* from t as a',
  ]) {
    expect(hasSelectStar(yes), yes).toBe(true);
  }
  for (const no of [
    'SELECT id FROM t',
    // Multiplication, not a projection: nothing follows the star that makes it
    // one, which is what the lookahead is for.
    'SELECT 2 * price FROM t',
    'SELECT count(*) FROM t',
    // No FROM at all, so there is no result shape to break.
    'SELECT *',
    '',
  ]) {
    expect(hasSelectStar(no), no).toBe(false);
  }
});
