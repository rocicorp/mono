import {describe, expect, test} from 'vitest';
import {compileInline} from './sql-inline.ts';
import {sql} from './sql.ts';

describe('SQL inline formatter', () => {
  test('inlines null values', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${null}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = NULL');
  });

  test('inlines string values with proper escaping', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${'hello'}`;
    const result = compileInline(query);
    expect(result).toBe("SELECT * FROM foo WHERE a = 'hello'");
  });

  test('escapes single quotes in strings', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${"O'Reilly"}`;
    const result = compileInline(query);
    expect(result).toBe("SELECT * FROM foo WHERE a = 'O''Reilly'");
  });

  test('escapes multiple single quotes in strings', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${"it's a test's test"}`;
    const result = compileInline(query);
    expect(result).toBe("SELECT * FROM foo WHERE a = 'it''s a test''s test'");
  });

  test('inlines number values', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${42}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = 42');
  });

  test('inlines negative number values', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${-123.45}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = -123.45');
  });

  test('inlines boolean true as 1', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${true}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = 1');
  });

  test('inlines boolean false as 0', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${false}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = 0');
  });

  test('inlines array values as JSON', () => {
    const query = sql`SELECT * FROM foo WHERE a IN (SELECT value FROM json_each(${[
      1, 2, 3,
    ]}))`;
    const result = compileInline(query);
    expect(result).toBe(
      "SELECT * FROM foo WHERE a IN (SELECT value FROM json_each('[1,2,3]'))",
    );
  });

  test('inlines array of strings as JSON with escaped quotes', () => {
    const query = sql`SELECT * FROM foo WHERE a IN (SELECT value FROM json_each(${[
      'foo',
      'bar',
    ]}))`;
    const result = compileInline(query);
    expect(result).toBe(
      `SELECT * FROM foo WHERE a IN (SELECT value FROM json_each('["foo","bar"]'))`,
    );
  });

  test('handles multiple inline values in one query', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${'test'} AND b = ${42} AND c = ${true}`;
    const result = compileInline(query);
    expect(result).toBe(
      "SELECT * FROM foo WHERE a = 'test' AND b = 42 AND c = 1",
    );
  });

  test('handles undefined as PLACEHOLDER IMPORTANT! MUST KEEP THIS FOR PLANNING', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${undefined}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = ?');
  });

  test('handles object values as JSON', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${{key: 'value'}}`;
    const result = compileInline(query);
    expect(result).toBe(`SELECT * FROM foo WHERE a = '{"key":"value"}'`);
  });

  test('handles empty string', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${''}`;
    const result = compileInline(query);
    expect(result).toBe("SELECT * FROM foo WHERE a = ''");
  });

  test('handles zero', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${0}`;
    const result = compileInline(query);
    expect(result).toBe('SELECT * FROM foo WHERE a = 0');
  });

  test('handles special SQL injection attempt (should be safely escaped)', () => {
    const query = sql`SELECT * FROM foo WHERE a = ${"'; DROP TABLE foo; --"}`;
    const result = compileInline(query);
    // Should be safely escaped with doubled quotes
    expect(result).toBe("SELECT * FROM foo WHERE a = '''; DROP TABLE foo; --'");
  });
});
