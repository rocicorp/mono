import {test, expect} from 'vitest';
import {isTableIgnored} from './ignored-tables.ts';

test('isTableIgnored matches fully qualified names', () => {
  const ignoredTables = new Set(['public.audit_logs', 'staging.temp_data']);
  
  expect(isTableIgnored({schema: 'public', name: 'audit_logs'}, ignoredTables)).toBe(true);
  expect(isTableIgnored({schema: 'staging', name: 'temp_data'}, ignoredTables)).toBe(true);
  expect(isTableIgnored({schema: 'public', name: 'users'}, ignoredTables)).toBe(false);
  expect(isTableIgnored({schema: 'public', name: 'temp_data'}, ignoredTables)).toBe(false);
  expect(isTableIgnored({schema: 'staging', name: 'audit_logs'}, ignoredTables)).toBe(false);
});

test('isTableIgnored with empty set', () => {
  const ignoredTables = new Set<string>();
  
  expect(isTableIgnored({schema: 'public', name: 'anything'}, ignoredTables)).toBe(false);
});