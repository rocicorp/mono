import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {
  SimpleCondition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import {createPredicate} from './filter.ts';
import {cases} from './like-test-cases.ts';

test('basics', () => {
  // nulls and undefined are false in all conditions except IS NULL and IS NOT NULL
  fc.assert(
    fc.property(
      fc.oneof(fc.constant(null), fc.constant(undefined)),
      fc.oneof(
        fc.constant('='),
        fc.constant('!='),
        fc.constant('<'),
        fc.constant('<='),
        fc.constant('>'),
        fc.constant('>='),
        fc.constant('LIKE'),
        fc.constant('NOT LIKE'),
        fc.constant('ILIKE'),
        fc.constant('NOT ILIKE'),
      ),
      // hexastring to avoid sending escape chars to like
      fc.oneof(fc.hexaString(), fc.double(), fc.boolean(), fc.constant(null)),
      (a, operator, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          left: {
            type: 'column',
            name: 'foo',
          },
          op: operator as SimpleOperator,
          right: {
            type: 'literal',
            value: b,
          },
        };
        const predicate = createPredicate(condition);
        expect(predicate({foo: a})).toBe(false);
      },
    ),
  );

  let condition: SimpleCondition = {
    type: 'simple',
    left: {
      type: 'column',
      name: 'foo',
    },
    op: 'IS',
    right: {
      type: 'literal',
      value: null,
    },
  };
  let predicate = createPredicate(condition);
  expect(predicate({foo: null})).toBe(true);
  expect(predicate({foo: 1})).toBe(false);
  expect(predicate({foo: 'null'})).toBe(false);
  expect(predicate({foo: true})).toBe(false);
  expect(predicate({foo: false})).toBe(false);

  condition = {
    type: 'simple',
    left: {
      type: 'column',
      name: 'foo',
    },
    op: 'IS NOT',
    right: {
      type: 'literal',
      value: null,
    },
  };
  predicate = createPredicate(condition);
  expect(predicate({foo: null})).toBe(false);
  expect(predicate({foo: 1})).toBe(true);
  expect(predicate({foo: 'null'})).toBe(true);
  expect(predicate({foo: true})).toBe(true);
  expect(predicate({foo: false})).toBe(true);

  // equality operators work across types (they use === / !==)
  fc.assert(
    fc.property(
      fc.oneof(fc.boolean(), fc.double(), fc.string()),
      fc.oneof(fc.constant('='), fc.constant('!=')),
      fc.oneof(fc.boolean(), fc.double(), fc.string()),
      (a, op, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          left: {
            type: 'column',
            name: 'foo',
          },
          op: op as SimpleOperator,
          right: {
            type: 'literal',
            value: b,
          },
        };
        const predicate = createPredicate(condition);
        const jsOp = op === '=' ? '===' : '!==';
        // oxlint-disable-next-line no-eval -- legitimate use for dynamic test comparison
        expect(predicate({foo: a})).toBe(eval(`a ${jsOp} b`));
      },
    ),
  );

  // ordered operators compare same-typed values via compareValues. For numbers
  // this matches JS ordering. String ordering uses UTF-8 (compareUTF8) and is
  // covered separately below; mixed-type ordered comparisons are unsupported
  // (compareValues throws), matching ORDER BY / SQLite, so they aren't exercised.
  fc.assert(
    fc.property(
      fc.double(),
      fc.oneof(
        fc.constant('<'),
        fc.constant('<='),
        fc.constant('>'),
        fc.constant('>='),
      ),
      fc.double(),
      (a, op, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          left: {
            type: 'column',
            name: 'foo',
          },
          op: op as SimpleOperator,
          right: {
            type: 'literal',
            value: b,
          },
        };
        const predicate = createPredicate(condition);
        // oxlint-disable-next-line no-eval -- legitimate use for dynamic test comparison
        expect(predicate({foo: a})).toBe(eval(`a ${op} b`));
      },
    ),
  );
});

test('like', () => {
  for (const {pattern, flags, inputs} of cases) {
    for (const [input, expected] of inputs) {
      const condition: SimpleCondition = {
        type: 'simple',
        left: {
          type: 'column',
          name: 'foo',
        },
        op: flags ? 'ILIKE' : 'LIKE',
        right: {
          type: 'literal',
          value: pattern,
        },
      };
      const predicate = createPredicate(condition);
      expect(predicate({foo: input})).toBe(expected);
    }
  }
});

test('json path', () => {
  const row = {
    metadata: {
      priority: 'high',
      count: 3,
      flagged: true,
      nested: {zip: '94110'},
      tags: ['a', 'b'],
      maybeNull: null,
    },
  };

  const p = (op: SimpleOperator, path: (string | number)[], value: unknown) =>
    createPredicate({
      type: 'simple',
      op,
      left: {type: 'json', value: {type: 'column', name: 'metadata'}, path},
      // oxlint-disable-next-line no-explicit-any
      right: {type: 'literal', value: value as any},
    });

  // string leaf
  expect(p('=', ['priority'], 'high')(row)).toBe(true);
  expect(p('=', ['priority'], 'low')(row)).toBe(false);
  expect(p('!=', ['priority'], 'low')(row)).toBe(true);
  // number leaf
  expect(p('>', ['count'], 2)(row)).toBe(true);
  expect(p('<', ['count'], 2)(row)).toBe(false);
  // boolean leaf
  expect(p('=', ['flagged'], true)(row)).toBe(true);
  expect(p('=', ['flagged'], false)(row)).toBe(false);
  // nested object
  expect(p('=', ['nested', 'zip'], '94110')(row)).toBe(true);
  // array index
  expect(p('=', ['tags', 0], 'a')(row)).toBe(true);
  expect(p('=', ['tags', 1], 'a')(row)).toBe(false);
  // LIKE on a string leaf
  expect(p('LIKE', ['priority'], 'hi%')(row)).toBe(true);
  expect(p('LIKE', ['priority'], 'lo%')(row)).toBe(false);
  // IN
  expect(p('IN', ['priority'], ['high', 'med'])(row)).toBe(true);
  expect(p('IN', ['priority'], ['low', 'med'])(row)).toBe(false);

  // Missing key: non-match for value ops; treated as null for IS (parity with
  // SQLite json_extract -> NULL).
  expect(p('=', ['missing'], 'x')(row)).toBe(false);
  expect(p('IS', ['missing'], null)(row)).toBe(true);
  expect(p('IS NOT', ['missing'], null)(row)).toBe(false);
  // Explicit JSON null leaf.
  expect(p('IS', ['maybeNull'], null)(row)).toBe(true);
  expect(p('IS NOT', ['maybeNull'], null)(row)).toBe(false);
  // Navigating through a null/absent intermediate.
  expect(p('=', ['maybeNull', 'deep'], 'x')(row)).toBe(false);
  expect(p('IS', ['maybeNull', 'deep'], null)(row)).toBe(true);
  // Top-level column entirely absent.
  expect(p('=', ['priority'], 'high')({})).toBe(false);
  expect(p('IS', ['priority'], null)({})).toBe(true);
});

test('and', () => {
  const predicate = createPredicate({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: false},
      },
    ],
  });
  expect(predicate({a: 4, b: true})).false;
  expect(predicate({a: 3, b: false})).false;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 4, b: false})).true;
});

test('or', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: false},
      },
    ],
  });
  expect(predicate({a: 4, b: true})).true;
  expect(predicate({a: 3, b: false})).true;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 4, b: false})).true;
});

test('empty and', () => {
  const predicate = createPredicate({
    type: 'and',
    conditions: [],
  });
  expect(predicate({a: 4, b: true})).true;
});

test('empty or', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [],
  });
  expect(predicate({a: 4, b: true})).false;
});

test('nested', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'a'},
            right: {type: 'literal', value: 3},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'b'},
            right: {type: 'literal', value: false},
          },
        ],
      },
    ],
  });
  expect(predicate({a: 4, b: true})).true;
  expect(predicate({a: 4, b: false})).true;
  expect(predicate({a: 3, b: false})).true;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 5, b: false})).false;
});

test('string range comparisons use UTF-8 / code-point order (consistent with ORDER BY and SQLite)', () => {
  // Raw JS string comparison uses UTF-16 code units, which disagree with the
  // UTF-8 / code-point order used by ORDER BY (compareValues -> compareUTF8) and
  // by SQLite for non-BMP characters. '｡' (U+FF61) sorts BEFORE
  // '\u{1F600}' (U+1F600 emoji) by code point, but AFTER it by UTF-16 code unit.
  const ltCondition: SimpleCondition = {
    type: 'simple',
    left: {type: 'column', name: 'foo'},
    op: '<',
    right: {type: 'literal', value: '\u{1F600}'},
  };
  expect(createPredicate(ltCondition)({foo: '｡'})).toBe(true);

  const gtCondition: SimpleCondition = {
    type: 'simple',
    left: {type: 'column', name: 'foo'},
    op: '>',
    right: {type: 'literal', value: '\u{1F600}'},
  };
  expect(createPredicate(gtCondition)({foo: '｡'})).toBe(false);

  // numeric comparisons are unaffected
  const numCondition: SimpleCondition = {
    type: 'simple',
    left: {type: 'column', name: 'foo'},
    op: '<',
    right: {type: 'literal', value: 5},
  };
  const numPredicate = createPredicate(numCondition);
  expect(numPredicate({foo: 3})).toBe(true);
  expect(numPredicate({foo: 9})).toBe(false);
});
