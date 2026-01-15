import {describe, expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from './create-builder.ts';

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        age: number(),
      })
      .primaryKey('id'),
    table('post')
      .columns({
        id: string(),
        title: string(),
        authorId: string(),
      })
      .primaryKey('id'),
    table('comment')
      .columns({
        id: string(),
        text: string(),
        postId: string(),
      })
      .primaryKey('id'),
  ],
});

describe('createBuilder', () => {
  test('returns query builders for tables', () => {
    const builder = createBuilder(schema);

    expect(builder.user).toBeDefined();
    expect(builder.post).toBeDefined();
    expect(builder.comment).toBeDefined();
  });

  test('query builders are usable', () => {
    const builder = createBuilder(schema);

    const userQuery = builder.user.where('id', '123');
    expect(userQuery).toBeDefined();

    const postQuery = builder.post.where('title', 'Hello');
    expect(postQuery).toBeDefined();
  });

  test('returns undefined for non-existent tables', () => {
    const builder = createBuilder(schema);

    // @ts-expect-error - testing runtime behavior for non-existent table
    expect(builder.nonExistent).toBeUndefined();
  });

  test('caches query builders', () => {
    const builder = createBuilder(schema);

    const user1 = builder.user;
    const user2 = builder.user;

    expect(user1).toBe(user2);
  });

  describe('enumeration', () => {
    test('Object.keys returns all table names', () => {
      const builder = createBuilder(schema);

      const keys = Object.keys(builder);
      expect(keys).toEqual(['user', 'post', 'comment']);
    });

    test('Object.entries returns all tables with query builders', () => {
      const builder = createBuilder(schema);

      const entries = Object.entries(builder);
      expect(entries.length).toBe(3);
      expect(entries.map(([key]) => key)).toEqual(['user', 'post', 'comment']);
      // Values should be query builders
      for (const [key, value] of entries) {
        expect(value).toBe(builder[key as keyof typeof builder]);
      }
    });

    test('Object.values returns all query builders', () => {
      const builder = createBuilder(schema);

      const values = Object.values(builder);
      expect(values.length).toBe(3);
      expect(values).toContain(builder.user);
      expect(values).toContain(builder.post);
      expect(values).toContain(builder.comment);
    });

    test('for-in loop iterates over all tables', () => {
      const builder = createBuilder(schema);

      const keys: string[] = [];
      for (const key in builder) {
        keys.push(key);
      }

      expect(keys).toEqual(['user', 'post', 'comment']);
    });

    test('in operator works for existing tables', () => {
      const builder = createBuilder(schema);

      // 'in' operator checks if property exists in the target tables object
      expect('user' in builder).toBe(true);
      expect('post' in builder).toBe(true);
      expect('nonExistent' in builder).toBe(false);
    });

    test('spread operator copies all tables', () => {
      const builder = createBuilder(schema);

      const spread = {...builder};
      expect(Object.keys(spread)).toEqual(['user', 'post', 'comment']);
      expect(spread.user).toBe(builder.user);
      expect(spread.post).toBe(builder.post);
      expect(spread.comment).toBe(builder.comment);
    });

    test('Object.getOwnPropertyDescriptor returns correct descriptor', () => {
      const builder = createBuilder(schema);

      const desc = Object.getOwnPropertyDescriptor(builder, 'user');
      expect(desc?.value).toBe(builder.user);
      expect(desc?.writable).toBe(true);
      expect(desc?.enumerable).toBe(true);
      expect(desc?.configurable).toBe(true);

      expect(
        Object.getOwnPropertyDescriptor(builder, 'nonExistent'),
      ).toBeUndefined();
    });

    test('Object.getOwnPropertyDescriptors returns all tables', () => {
      const builder = createBuilder(schema);

      const descs = Object.getOwnPropertyDescriptors(builder);
      expect(Object.keys(descs)).toEqual(['user', 'post', 'comment']);
      expect(descs.user.value).toBe(builder.user);
      expect(descs.post.value).toBe(builder.post);
      expect(descs.comment.value).toBe(builder.comment);
    });

    test('toString returns undefined (null prototype, no inherited methods)', () => {
      const builder = createBuilder(schema);

      // Accessing inherited methods returns undefined like any other non-existent table
      expect(builder.toString).toBeUndefined();
    });

    test('table named toString works correctly', () => {
      // Note: schema builder should use Object.create(null) or hasOwn to allow
      // tables named after Object.prototype methods. For now, use a workaround.
      const schemaWithToString = createSchema({
        tables: [
          table('toString')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
      });

      const builder = createBuilder(schemaWithToString);

      // Should return a query builder, not throw
      const query = builder.toString;
      expect(query).toBeDefined();

      // Should be usable as a query builder
      const filtered = query.where('id', '123');
      expect(filtered).toBeDefined();
    });

    test('Symbol.toStringTag is passed through to underlying tables object', () => {
      const builder = createBuilder(schema);

      // Symbol properties should be passed through to the underlying tables object
      // The tables object doesn't have a custom toStringTag, so it returns undefined
      expect(
        (builder as unknown as {[Symbol.toStringTag]: string})[
          Symbol.toStringTag
        ],
      ).toBeUndefined();
    });
  });
});
