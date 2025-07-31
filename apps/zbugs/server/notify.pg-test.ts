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

      CREATE TABLE "issueNotifications" (
        "userID" TEXT REFERENCES "user"(id),
        "issueID" TEXT REFERENCES "issue"(id),
        "subscribed" BOOLEAN DEFAULT true,
        "created" DOUBLE PRECISION DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000),
        PRIMARY KEY ("userID", "issueID")
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

      -- Insert notification subscriptions
      -- For issue-123 (public): user1 (creator), user2 (assignee/commenter), user3 (emoji reactor) are subscribed
      INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed") VALUES
        ('user1', 'issue-123', true),
        ('user2', 'issue-123', true),
        ('user3', 'issue-123', true);

      -- For issue-456 (internal): only user3 (crew member) should be subscribed
      INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed") VALUES
        ('user3', 'issue-456', true),
        ('user4', 'issue-456', true),  -- This user will be filtered out by role in the actual query
        ('user1', 'issue-456', true);  -- This user will be filtered out by role in the actual query
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
    it('should include all subscribed recipients for public issues', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4', // actor who performed the action
      );

      expect(recipients).toHaveLength(3);
      expect(recipients).toContain('user1@example.com'); // subscribed
      expect(recipients).toContain('user2@example.com'); // subscribed
      expect(recipients).toContain('user3@example.com'); // subscribed
    });

    it('should only include crew members for internal issues', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-456',
        'user2', // actor who performed the action
      );

      expect(recipients).toHaveLength(1);
      expect(recipients).toContain('user3@example.com'); // crew member
      expect(recipients).not.toContain('user1@example.com'); // not crew
      expect(recipients).not.toContain('user4@example.com'); // not crew
    });

    it('should exclude the actor from recipients', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user1', // user1 is the actor, should be excluded
      );

      expect(recipients).toHaveLength(2);
      expect(recipients).not.toContain('user1@example.com'); // actor excluded
      expect(recipients).toContain('user2@example.com'); // subscribed
      expect(recipients).toContain('user3@example.com'); // subscribed
    });

    it('should exclude the actor from recipients', async () => {
      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user2', // user2 is the actor, should be excluded
      );

      expect(recipients).toHaveLength(2);
      expect(recipients).not.toContain('user2@example.com'); // actor excluded
      expect(recipients).toContain('user1@example.com'); // subscribed
      expect(recipients).toContain('user3@example.com'); // subscribed
    });

    it('should filter out null emails', async () => {
      // Add a user with null email
      await db`
        INSERT INTO "user" (id, email, role) 
        VALUES ('user5', NULL, 'user')
      `;

      // Subscribe this user to notifications
      await db`
        INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed")
        VALUES ('user5', 'issue-123', true)
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4', // actor who performed the action
      );

      expect(recipients).toHaveLength(3);
      expect(recipients).toContain('user1@example.com');
      expect(recipients).toContain('user2@example.com');
      expect(recipients).toContain('user3@example.com');
      // user5 should not be included due to null email
    });

    it('should return empty array when no users are subscribed', async () => {
      // Create a new issue with no subscriptions
      await db`
        INSERT INTO "issue" (id, "creatorID", "assigneeID", visibility) 
        VALUES ('issue-789', 'user2', 'user2', 'public')
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-789',
        'user1',
      );

      expect(recipients).toHaveLength(0);
    });

    it('should not include unsubscribed users', async () => {
      // Unsubscribe user2 from issue-123
      await db`
        UPDATE "issueNotifications" 
        SET "subscribed" = false 
        WHERE "userID" = 'user2' AND "issueID" = 'issue-123'
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4',
      );

      expect(recipients).toHaveLength(2);
      expect(recipients).toContain('user1@example.com');
      expect(recipients).not.toContain('user2@example.com'); // unsubscribed
      expect(recipients).toContain('user3@example.com');
    });

    it('should filter out malformed email addresses', async () => {
      // Add a user with a malformed email
      await db`
        INSERT INTO "user" (id, email, role)
        VALUES ('user6', 'invalid-email@test!!.com', 'user')
      `;

      await db`
        INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed")
        VALUES ('user6', 'issue-123', true)
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4', // actor who performed the action
      );

      expect(recipients).toHaveLength(3); // existing 3 valid emails
      expect(recipients).not.toContain('invalid-email');
    });

    it('should filter out empty email addresses', async () => {
      // Add a user with a malformed email
      await db`
        INSERT INTO "user" (id, email, role)
        VALUES ('user6', '', 'user')
      `;

      await db`
        INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed")
        VALUES ('user6', 'issue-123', true)
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4', // actor who performed the action
      );

      expect(recipients).toHaveLength(3); // existing 3 valid emails
      expect(recipients).not.toContain('');
    });

    it('should trim whitespace around email addresses', async () => {
      // Add a user whose email has leading/trailing whitespace
      await db`
        INSERT INTO "user" (id, email, role)
        VALUES ('user7', '  user7@example.com  ', 'user')
      `;

      await db`
        INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed")
        VALUES ('user7', 'issue-123', true)
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4',
      );

      expect(recipients).toHaveLength(4); // 3 existing + 1 new
      expect(recipients).toContain('user7@example.com');
    });

    it('should not include duplicate email addresses across multiple users', async () => {
      // Add two users sharing the same email address
      await db.unsafe(`
        INSERT INTO "user" (id, email, role) VALUES
          ('user8', 'shared@example.com', 'user'),
          ('user9', 'shared@example.com', 'user');
      `);

      await db`
        INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed") VALUES
          ('user8', 'issue-123', true),
          ('user9', 'issue-123', true)
      `;

      const recipients = await gatherRecipients(
        createMockTx(db),
        'issue-123',
        'user4',
      );

      // It should only include one occurrence of the shared email
      const sharedOccurrences = recipients.filter(
        e => e === 'shared@example.com',
      ).length;
      expect(sharedOccurrences).toBe(1);
      expect(recipients).toHaveLength(4); // 3 existing + 1 shared email
    });
  });
});
