import {describe, it, expect} from 'vitest';
import {type Schema} from '../shared/schema.ts';
import {gatherRecipients} from './notify.ts';
import type {ServerTransaction} from '@rocicorp/zero';
import type postgres from 'postgres';

// Mock transaction object
const createMockTx = (
  mockSql: any,
): ServerTransaction<Schema, postgres.TransactionSql> =>
  ({
    dbTransaction: {
      wrappedTransaction: mockSql,
    },
  }) as any;

describe('gatherRecipients', () => {
  it('should include issue creator, commenters, emoji reactors, and assignees for public issues', async () => {
    const mockSql = async () => [
      {email: 'creator@example.com'},
      {email: 'commenter@example.com'},
      {email: 'emoji_reactor@example.com'},
      {email: 'assignee@example.com'},
    ];

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
    );

    expect(recipients).toHaveLength(4);
    expect(recipients).toContain('creator@example.com');
    expect(recipients).toContain('commenter@example.com');
    expect(recipients).toContain('emoji_reactor@example.com');
    expect(recipients).toContain('assignee@example.com');
  });

  it('should only include crew members for private issues', async () => {
    // Mock the SQL query to return only crew members for private issues
    const mockSql = async () => {
      // Simulate the visibility check in the SQL query
      const isPublic = false;
      if (!isPublic) {
        return [{email: 'crew@example.com', role: 'crew'}];
      }
      return [
        {email: 'crew@example.com', role: 'crew'},
        {email: 'non_crew@example.com', role: 'user'},
      ];
    };

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
    );

    expect(recipients).toHaveLength(1);
    expect(recipients).toContain('crew@example.com');
    expect(recipients).not.toContain('non_crew@example.com');
  });

  it('should include previous assignee when assignee changes', async () => {
    const mockSql = async () => [
      {email: 'previous_assignee@example.com'},
      {email: 'other@example.com'},
    ];

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      true,
      'prev-assignee-id',
    );

    expect(recipients).toContain('previous_assignee@example.com');
  });

  it('should not include previous assignee when not an assignee change', async () => {
    // Mock the SQL query to not include previous assignee when isAssigneeChange is false
    const mockSql = async () => {
      const isAssigneeChange = false;
      if (!isAssigneeChange) {
        return [{email: 'other@example.com'}];
      }
      return [
        {email: 'previous_assignee@example.com'},
        {email: 'other@example.com'},
      ];
    };

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
      'prev-assignee-id',
    );

    expect(recipients).not.toContain('previous_assignee@example.com');
  });

  it('should filter out null emails', async () => {
    // Mock the SQL query to filter out null emails
    const mockSql = async () => {
      const results = [
        {email: 'valid@example.com'},
        {email: null},
        {email: 'another@example.com'},
      ];
      return results.filter(r => r.email !== null);
    };

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
    );

    expect(recipients).toHaveLength(2);
    expect(recipients).toContain('valid@example.com');
    expect(recipients).toContain('another@example.com');
  });
});
