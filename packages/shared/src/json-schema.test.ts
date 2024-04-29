import {expect, test} from 'vitest';
import {assert} from './asserts.js';
import {jsonObjectSchema, jsonSchema} from './json-schema.js';
import * as v from './valita.js';
import {parse} from './valita.js';

test('json schema', () => {
  const t = <T>(s: v.Type<T>, v: unknown, message?: string) => {
    let ex;
    try {
      const parsed = parse(v, s);
      expect(parsed).toBe(v);
    } catch (err) {
      ex = err;
    }

    if (message !== undefined) {
      assert(ex instanceof TypeError);
      expect(ex.message).toBe(message);
    } else {
      expect(ex).toBe(undefined);
    }
  };

  {
    t(jsonSchema, 1);
    t(jsonSchema, '');
    t(jsonSchema, 'hello');
    t(jsonSchema, true);
    t(jsonSchema, false);
    t(jsonSchema, null);
    t(jsonSchema, []);
    t(jsonSchema, {});
    t(jsonSchema, [1, 2, 3]);
    t(jsonSchema, {a: 1, b: 2});
    t(jsonSchema, {a: 1, b: 2, c: [1, 2, 3]});
  }

  {
    const s = v.object({
      a: v.object({
        b: jsonSchema,
      }),
    });
    t(s, {a: {b: 1n}}, 'Not a JSON value at a.b. Got 1n');
    t(s, {a: {b: {x: {y: 2n}}}}, 'Not a JSON value at a.b.x.y. Got 2n');
    t(s, {a: {b: undefined}}, 'Not a JSON value at a.b. Got undefined');
  }
});

test('json object schema', () => {
  const t = <T>(s: v.Type<T>, v: unknown, message?: string) => {
    let ex;
    try {
      const parsed = parse(v, s);
      expect(parsed).toBe(v);
    } catch (err) {
      ex = err;
    }

    if (message !== undefined) {
      assert(ex instanceof TypeError);
      expect(ex.message).toBe(message);
    } else {
      expect(ex).toBe(undefined);
    }
  };

  {
    t(jsonObjectSchema, 1, 'Not a JSON object. Got 1');
    t(jsonObjectSchema, '', 'Not a JSON object. Got ""');
    t(jsonObjectSchema, 'hello', 'Not a JSON object. Got "hello"');
    t(jsonObjectSchema, true, 'Not a JSON object. Got true');
    t(jsonObjectSchema, false, 'Not a JSON object. Got false');
    t(jsonObjectSchema, null, 'Not a JSON object. Got null');
    t(jsonObjectSchema, []);
    t(jsonObjectSchema, {});
    t(jsonObjectSchema, [1, 2, 3]);
    t(jsonObjectSchema, {a: 1, b: 2});
    t(jsonObjectSchema, {a: 1, b: 2, c: [1, 2, 3]});
  }

  {
    const s = v.object({
      a: v.object({
        b: jsonObjectSchema,
      }),
    });
    t(s, {a: {b: 1n}}, 'Not a JSON object at a.b. Got 1n');
    t(s, {a: {b: {x: {y: 2n}}}}, 'Not a JSON object at a.b.x.y. Got 2n');
    t(s, {a: {b: undefined}}, 'Not a JSON object at a.b. Got undefined');
  }
});
