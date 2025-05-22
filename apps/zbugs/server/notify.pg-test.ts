import {describe, it, expect, beforeEach} from 'vitest';
import {type Schema} from '../shared/schema.ts';
import {gatherRecipients} from './notify.ts';
import type {ServerTransaction} from '@rocicorp/zero';
import type postgres from 'postgres';
import {
  testDBs,
  getConnectionURI,
} from '../../../packages/zero-cache/src/test/db.ts';

describe('notify', () => {
  let db: postgres.Sql;
  let dbURI: string;

  beforeEach(async () => {
    db = await testDBs.create('notify_test');
    dbURI = getConnectionURI(db);

    // Set up test tables
    await db.unsafe(`
      CREATE TABLE "user" (
        id TEXT PRIMARY KEY,
        email TEXT,
        role TEXT
      );

      CREATE TABLE "issue" (
        id TEXT PRIMARY KEY,
        "creatorID" TEXT REFERENCES "user"(id),
        "assigneeID" TEXT REFERENCES "user"(id),
        visibility TEXT
      );

      CREATE TABLE "comment" (
        id TEXT PRIMARY KEY,
        "issueID" TEXT REFERENCES "issue"(id),
        "creatorID" TEXT REFERENCES "user"(id),
        body TEXT
      );

      CREATE TABLE "emoji" (
        id TEXT PRIMARY KEY,
        "subjectID" TEXT,
        "creatorID" TEXT REFERENCES "user"(id),
        unicode TEXT
      );

      -- Insert test data
      INSERT INTO "user" (id, email, role) VALUES
        ('user1', 'user1@example.com', 'user'),
        ('user2', 'user2@example.com', 'user'),
        ('user3', 'user3@example.com', 'crew'),
        ('user4', 'user4@example.com', 'user');

      INSERT INTO "issue" (id, "creatorID", "assigneeID", visibility) VALUES
        ('issue-123', 'user1', 'user2', 'public'),
        ('issue-456', 'user3', 'user4', 'internal');

      INSERT INTO "comment" (id, "issueID", "creatorID", body) VALUES
        ('comment1', 'issue-123', 'user2', 'test comment'),
        ('comment2', 'issue-456', 'user4', 'test comment');

      INSERT INTO "emoji" (id, "subjectID", "creatorID", unicode) VALUES
        ('emoji1', 'issue-123', 'user3', '👍'),
        ('emoji2', 'issue-456', 'user1', '👍');
    `);

    return async () => {
      await testDBs.drop(db);
    };
  });

  const createMockTx = (
    sql: postgres.Sql,
  ): ServerTransaction<Schema, postgres.TransactionSql> =>
    ({
      dbTransaction: {
        wrappedTransaction: sql,
      },
    }) as any;

  describe('gatherRecipients', () => {
    it('should include issue creator, commenters, emoji reactors, and assignees for public issues', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
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
        createMockTx(db),
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
        createMockTx(db),
        'issue-123',
        true,
        'user1',
      );

      expect(recipients).toContain('user1@example.com'); // previous assignee
    });

    it('should not include previous assignee when not an assignee change', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
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
      // Add a user with null email
      await db`
        INSERT INTO "user" (id, email, role) 
        VALUES ('user5', NULL, 'user')
      `;

      // Add this user as a commenter
      await db`
        INSERT INTO "comment" (id, "issueID", "creatorID", body)
        VALUES ('comment3', 'issue-123', 'user5', 'test comment')
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        false,
      );

      expect(recipients).toHaveLength(3);
      expect(recipients).toContain('user1@example.com');
      expect(recipients).toContain('user2@example.com');
      expect(recipients).toContain('user3@example.com');
    });
  });
});
