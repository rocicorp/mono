import {describe, expect, test} from 'vitest';
import type {
  MessageInsert,
  MessageUpdate,
  MessageDelete,
  MessageTruncate,
  MessageBegin,
  MessageCommit,
} from '../change-source/protocol/current/data.ts';
import {getTableFromChange} from './storer.ts';

describe('change-streamer/storer table stats', () => {
  describe('getTableFromChange', () => {
    test('extracts table from insert', () => {
      const change: MessageInsert = {
        tag: 'insert',
        relation: {
          schema: 'public',
          name: 'users',
          keyColumns: ['id'],
        },
        new: {id: '1', name: 'test'},
      };
      expect(getTableFromChange(change)).toBe('public.users');
    });

    test('extracts table from update', () => {
      const change: MessageUpdate = {
        tag: 'update',
        relation: {
          schema: 'myschema',
          name: 'orders',
          keyColumns: ['id'],
        },
        key: null,
        new: {id: '1', total: 100},
      };
      expect(getTableFromChange(change)).toBe('myschema.orders');
    });

    test('extracts table from delete', () => {
      const change: MessageDelete = {
        tag: 'delete',
        relation: {
          schema: 'public',
          name: 'sessions',
          keyColumns: ['id'],
        },
        key: {id: '1'},
      };
      expect(getTableFromChange(change)).toBe('public.sessions');
    });

    test('extracts tables from truncate', () => {
      const change: MessageTruncate = {
        tag: 'truncate',
        relations: [
          {schema: 'public', name: 'logs', keyColumns: ['id']},
          {schema: 'audit', name: 'events', keyColumns: ['id']},
        ],
      };
      expect(getTableFromChange(change)).toBe('public.logs, audit.events');
    });

    test('returns null for begin', () => {
      const change: MessageBegin = {tag: 'begin'};
      expect(getTableFromChange(change as never)).toBe(null);
    });

    test('returns null for commit', () => {
      const change: MessageCommit = {tag: 'commit'};
      expect(getTableFromChange(change as never)).toBe(null);
    });
  });
});
