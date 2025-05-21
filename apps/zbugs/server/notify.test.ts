import {describe, it, expect} from 'vitest';
import {type Schema} from '../shared/schema.ts';
import {gatherRecipients} from './notify.ts';
import type {ServerTransaction} from '@rocicorp/zero';
import type postgres from 'postgres';

// Mock database state
const mockDB = {
  users: [
    {id: 'user1', email: 'user1@example.com', role: 'user'},
    {id: 'user2', email: 'user2@example.com', role: 'user'},
    {id: 'user3', email: 'user3@example.com', role: 'crew'},
    {id: 'user4', email: 'user4@example.com', role: 'user'},
  ],
  issues: [
    {
      id: 'issue-123',
      creatorID: 'user1',
      assigneeID: 'user2',
      visibility: 'public',
    },
    {
      id: 'issue-456',
      creatorID: 'user3',
      assigneeID: 'user4',
      visibility: 'private',
    },
  ],
  comments: [
    {
      id: 'comment1',
      issueID: 'issue-123',
      creatorID: 'user2',
      body: 'test comment',
    },
    {
      id: 'comment2',
      issueID: 'issue-456',
      creatorID: 'user4',
      body: 'test comment',
    },
  ],
  emojis: [
    {id: 'emoji1', subjectID: 'issue-123', creatorID: 'user3', unicode: '👍'},
    {id: 'emoji2', subjectID: 'issue-456', creatorID: 'user1', unicode: '👍'},
  ],
};

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
  const mockSql = async (strings: TemplateStringsArray, ...values: any[]) => {
    // Extract parameters from the query
    const issueID = values[0];
    const isAssigneeChange = values[1];
    const previousAssigneeID = values[2];

    // Get issue info
    const issue = mockDB.issues.find(i => i.id === issueID);
    if (!issue) return [];

    // Get all potential recipients
    const recipients = new Set<string>();

    // Add creator
    const creator = mockDB.users.find(u => u.id === issue.creatorID);
    if (creator?.email) recipients.add(creator.email);

    // Add assignee
    const assignee = mockDB.users.find(u => u.id === issue.assigneeID);
    if (assignee?.email) recipients.add(assignee.email);

    // Add commenters
    mockDB.comments
      .filter(c => c.issueID === issueID)
      .forEach(comment => {
        const commenter = mockDB.users.find(u => u.id === comment.creatorID);
        if (commenter?.email) recipients.add(commenter.email);
      });

    // Add emoji reactors
    mockDB.emojis
      .filter(e => e.subjectID === issueID)
      .forEach(emoji => {
        const reactor = mockDB.users.find(u => u.id === emoji.creatorID);
        if (reactor?.email) recipients.add(reactor.email);
      });

    // Add emoji reactors on comments
    mockDB.comments
      .filter(c => c.issueID === issueID)
      .forEach(comment => {
        mockDB.emojis
          .filter(e => e.subjectID === comment.id)
          .forEach(emoji => {
            const reactor = mockDB.users.find(u => u.id === emoji.creatorID);
            if (reactor?.email) recipients.add(reactor.email);
          });
      });

    // Add previous assignee if this is an assignee change
    if (isAssigneeChange && previousAssigneeID) {
      const previousAssignee = mockDB.users.find(
        u => u.id === previousAssigneeID,
      );
      if (previousAssignee?.email) recipients.add(previousAssignee.email);
    }

    // Filter based on visibility
    if (issue.visibility !== 'public') {
      return Array.from(recipients)
        .filter(email => {
          const user = mockDB.users.find(u => u.email === email);
          return user?.role === 'crew';
        })
        .map(email => ({email}));
    }

    return Array.from(recipients).map(email => ({email}));
  };

  it('should include issue creator, commenters, emoji reactors, and assignees for public issues', async () => {
    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
    );

    expect(recipients).toHaveLength(3);
    expect(recipients).toContain('user1@example.com'); // creator
    expect(recipients).toContain('user2@example.com'); // assignee and commenter
    expect(recipients).toContain('user3@example.com'); // emoji reactor
  });

  it('should only include crew members for private issues', async () => {
    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-456',
      false,
    );

    expect(recipients).toHaveLength(1);
    expect(recipients).toContain('user3@example.com'); // only crew member
    expect(recipients).not.toContain('user4@example.com'); // not crew
    expect(recipients).not.toContain('user1@example.com'); // not crew
  });

  it('should include previous assignee when assignee changes', async () => {
    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      true,
      'user1',
    );

    expect(recipients).toContain('user1@example.com'); // previous assignee
  });

  it('should not include previous assignee when not an assignee change', async () => {
    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
      'user1',
    );

    expect(recipients).toHaveLength(3);
    expect(recipients).toContain('user1@example.com'); // creator
    expect(recipients).toContain('user2@example.com'); // assignee and commenter
    expect(recipients).toContain('user3@example.com'); // emoji reactor
  });

  it('should filter out null emails', async () => {
    // Add a user with null email to test data
    const originalUsers = [...mockDB.users];
    mockDB.users.push({id: 'user5', email: null, role: 'user'});

    // Add this user as a commenter
    const originalComments = [...mockDB.comments];
    mockDB.comments.push({
      id: 'comment3',
      issueID: 'issue-123',
      creatorID: 'user5',
      body: 'test comment',
    });

    const recipients = await gatherRecipients(
      createMockTx(mockSql),
      'issue-123',
      false,
    );

    // Restore original data
    mockDB.users = originalUsers;
    mockDB.comments = originalComments;

    expect(recipients).toHaveLength(3);
    expect(recipients).toContain('user1@example.com');
    expect(recipients).toContain('user2@example.com');
    expect(recipients).toContain('user3@example.com');
  });
});
