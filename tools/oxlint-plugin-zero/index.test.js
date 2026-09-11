import {describe, expect, test} from 'vitest';
import {
  bareShimSpawnCommand,
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

// Minimal ESTree-shaped literals; only the fields the predicates read.
const literal = value => ({type: 'Literal', value});

const spawnCall = (name, commandValue, options) => ({
  type: 'CallExpression',
  callee: {type: 'Identifier', name},
  arguments: [
    commandValue === undefined ? undefined : literal(commandValue),
    {type: 'ArrayExpression', elements: []},
    options,
  ],
});
const objectWith = (keyName, value = {type: 'Literal', value: true}) => ({
  type: 'ObjectExpression',
  properties: [{type: 'Property', key: {name: keyName}, value}],
});

describe('bareShimSpawnCommand', () => {
  test.each(['spawn', 'spawnSync', 'execFile', 'execFileSync'])(
    'flags a bare shim command via %s',
    name => {
      expect(bareShimSpawnCommand(spawnCall(name, 'npm'))).toBe('npm');
    },
  );

  test('flags tsc, the case that breaks the build', () => {
    expect(bareShimSpawnCommand(spawnCall('spawn', 'tsc'))).toBe('tsc');
  });

  test('reads only the leading word of a command string', () => {
    expect(
      bareShimSpawnCommand(spawnCall('spawn', 'tsc -p tsconfig.json')),
    ).toBe('tsc');
  });

  test('ignores a real executable', () => {
    expect(bareShimSpawnCommand(spawnCall('spawn', 'git'))).toBeUndefined();
    expect(bareShimSpawnCommand(spawnCall('spawn', 'docker'))).toBeUndefined();
  });

  test('ignores a call that already passes an explicit shell option', () => {
    expect(
      bareShimSpawnCommand(spawnCall('spawn', 'npm', objectWith('shell'))),
    ).toBeUndefined();
  });

  test('finds the shell option in the two-argument form, where args is omitted', () => {
    // `args` is optional in every one of these signatures, so the options bag can
    // sit at index 1. Reading it positionally at index 2 reported these despite
    // the opt-out — an error on code that had already made the choice.
    for (const name of ['spawn', 'spawnSync', 'execFile', 'execFileSync']) {
      const node = {
        type: 'CallExpression',
        callee: {type: 'Identifier', name},
        arguments: [literal('npm'), objectWith('shell')],
      };
      expect(bareShimSpawnCommand(node)).toBeUndefined();
    }
  });

  test('still flags the two-argument form when the options do not opt in', () => {
    const node = {
      type: 'CallExpression',
      callee: {type: 'Identifier', name: 'spawn'},
      arguments: [literal('npm'), objectWith('cwd')],
    };
    expect(bareShimSpawnCommand(node)).toBe('npm');
  });

  test.each([false, null, 0, ''])(
    'flags a call whose shell option is the falsy literal %p',
    value => {
      // `shell: false` is the configuration that fails on Windows, so treating
      // the key's presence as the opt-out would exempt exactly the call this
      // rule exists to report.
      expect(
        bareShimSpawnCommand(
          spawnCall(
            'spawn',
            'npm',
            objectWith('shell', {
              type: 'Literal',
              value,
            }),
          ),
        ),
      ).toBe('npm');
    },
  );

  test.each([
    ['undefined', {type: 'Identifier', name: 'undefined'}],
    ['void 0', {type: 'UnaryExpression', operator: 'void'}],
  ])('flags a shell option written as %s', (_name, value) => {
    // Neither is a Literal, so a Literal-only falsy check exempts them — and both
    // are byte-for-byte the configuration that fails, same as `shell: false`.
    expect(
      bareShimSpawnCommand(
        spawnCall('spawn', 'npm', objectWith('shell', value)),
      ),
    ).toBe('npm');
  });

  test('exempts a shell option whose value is not statically falsy', () => {
    // A variable could be either; assuming it is falsy would report code that
    // may well be correct.
    expect(
      bareShimSpawnCommand(
        spawnCall(
          'spawn',
          'npm',
          objectWith('shell', {
            type: 'Identifier',
            name: 'useShell',
          }),
        ),
      ),
    ).toBeUndefined();
  });

  test('treats a spread options object as opting in', () => {
    // Its contents are not knowable here, so staying quiet is the cheaper error:
    // a missed finding costs less than an error on correct code.
    const node = {
      type: 'CallExpression',
      callee: {type: 'Identifier', name: 'spawn'},
      arguments: [
        literal('npm'),
        {type: 'ArrayExpression', elements: []},
        {
          type: 'ObjectExpression',
          properties: [
            {
              type: 'SpreadElement',
              argument: {type: 'Identifier', name: 'base'},
            },
          ],
        },
      ],
    };
    expect(bareShimSpawnCommand(node)).toBeUndefined();
  });

  test('an unrelated option does not exempt the call', () => {
    expect(
      bareShimSpawnCommand(spawnCall('spawn', 'npm', objectWith('stdio'))),
    ).toBe('npm');
  });

  test('ignores a non-literal command, which it cannot resolve statically', () => {
    const node = spawnCall('spawn', undefined);
    node.arguments[0] = {type: 'Identifier', name: 'litestream'};
    expect(bareShimSpawnCommand(node)).toBeUndefined();
  });

  test('ignores unrelated calls', () => {
    expect(bareShimSpawnCommand(spawnCall('execSync', 'npm'))).toBeUndefined();
  });
});

test('bareShimSpawnCommand flags the member form childProcess.spawn(...)', () => {
  const node = spawnCall('spawn', 'npm');
  node.callee = {
    type: 'MemberExpression',
    computed: false,
    object: {type: 'Identifier', name: 'childProcess'},
    property: {type: 'Identifier', name: 'spawn'},
  };
  expect(bareShimSpawnCommand(node)).toBe('npm');
});

test('bareShimSpawnCommand ignores a computed member callee it cannot read', () => {
  const node = spawnCall('spawn', 'npm');
  node.callee = {
    type: 'MemberExpression',
    computed: true,
    object: {type: 'Identifier', name: 'childProcess'},
    property: {type: 'Identifier', name: 'spawn'},
  };
  expect(bareShimSpawnCommand(node)).toBeUndefined();
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
