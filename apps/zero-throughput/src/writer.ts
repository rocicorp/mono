import type postgres from 'postgres';
import type {BenchmarkConfig} from './config.ts';
import type {BenchmarkDB} from './db.ts';
import {
  EMAIL_THREAD_COUNT,
  FORUM_CATEGORY_ID,
  FORUM_THREAD_COUNT,
  FORUM_USER_COUNT,
  REL_ACCOUNT_COUNT,
  REL_CONTACTS_PER_ACCOUNT,
  REL_ORG_ID,
  SHARED_OWNER_ID,
} from './profiles.ts';
import {nowMs, sleep} from './util.ts';

export type WriterStats = {
  readonly startedAtMs: number;
  readonly finishedAtMs: number;
  readonly committedRows: number;
  readonly committedTransactions: number;
  readonly highestCommittedSeq: number;
  readonly transactionLatencyMs: readonly number[];
};

type WriteSQL = BenchmarkDB | postgres.TransactionSql;

export class FixedRateWriter {
  readonly #sql: BenchmarkDB;
  readonly #config: BenchmarkConfig;
  readonly #payload: string;
  #highestCommittedSeq = 0;

  constructor(sql: BenchmarkDB, config: BenchmarkConfig) {
    this.#sql = sql;
    this.#config = config;
    this.#payload = 'x'.repeat(config.payloadBytes);
  }

  get highestCommittedSeq(): number {
    return this.#highestCommittedSeq;
  }

  async run(durationMs: number): Promise<WriterStats> {
    const startedAtMs = nowMs();
    const deadline = startedAtMs + durationMs;
    const transactionLatencyMs: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    let nextSeq = 1;
    let nextStart = startedAtMs;
    const intervalMs = (this.#config.batchSize / this.#config.writeRate) * 1000;

    while (nowMs() < deadline) {
      const delayMs = nextStart - nowMs();
      if (delayMs > 0) {
        await sleep(delayMs);
      }
      nextStart += intervalMs;

      const seqs = Array.from(
        {length: this.#config.batchSize},
        () => nextSeq++,
      );

      const txStart = nowMs();
      await this.#sql.begin(async tx => {
        for (const seq of seqs) {
          await this.#writeOne(tx, seq);
        }
      });
      transactionLatencyMs.push(nowMs() - txStart);
      committedRows += seqs.length;
      committedTransactions++;
      this.#highestCommittedSeq = nextSeq - 1;
    }

    return {
      startedAtMs,
      finishedAtMs: nowMs(),
      committedRows,
      committedTransactions,
      highestCommittedSeq: this.#highestCommittedSeq,
      transactionLatencyMs,
    };
  }

  async #writeOne(sql: WriteSQL, seq: number): Promise<void> {
    switch (this.#config.profile) {
      case 'feed-append': {
        await this.#writeFeedAppend(sql, seq);
        return;
      }
      case 'email': {
        await this.#writeEmail(sql, seq);
        return;
      }
      case 'forum': {
        await this.#writeForum(sql, seq);
        return;
      }
      case 'relational': {
        await this.#writeRelational(sql, seq);
        return;
      }
    }
  }

  async #writeFeedAppend(sql: WriteSQL, seq: number): Promise<void> {
    await sql`
      INSERT INTO zero_throughput_event
        (id, profile, shard, bucket, seq, payload)
      VALUES
        (
          ${`${this.#config.runID}-${seq}`},
          ${this.#config.profile},
          0,
          0,
          ${seq},
          ${sql.json({data: this.#payload})}
        )
    `;
  }

  async #writeEmail(sql: WriteSQL, seq: number): Promise<void> {
    const threadID = `email-thread-${seq % EMAIL_THREAD_COUNT}`;
    await sql`
      INSERT INTO zero_throughput_email_message
        (id, thread_id, owner_id, mailbox, sender_id, unread, body, seq)
      VALUES
        (
          ${`${this.#config.runID}-email-message-${seq}`},
          ${threadID},
          ${SHARED_OWNER_ID},
          'inbox',
          ${`sender-${seq % 16}`},
          true,
          ${this.#payload},
          ${seq}
        )
    `;
    await sql`
      UPDATE zero_throughput_email_thread
      SET
        seq = ${seq},
        written_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = ${threadID}
    `;
  }

  async #writeForum(sql: WriteSQL, seq: number): Promise<void> {
    const threadID = `forum-thread-${seq % FORUM_THREAD_COUNT}`;
    const authorID = `forum-user-${seq % FORUM_USER_COUNT}`;
    await sql`
      INSERT INTO zero_throughput_forum_post
        (id, thread_id, category_id, author_id, body, seq)
      VALUES
        (
          ${`${this.#config.runID}-forum-post-${seq}`},
          ${threadID},
          ${FORUM_CATEGORY_ID},
          ${authorID},
          ${this.#payload},
          ${seq}
        )
    `;
    await sql`
      UPDATE zero_throughput_forum_thread
      SET
        seq = ${seq},
        written_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = ${threadID}
    `;
    await sql`
      UPDATE zero_throughput_forum_category
      SET
        seq = ${seq},
        written_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = ${FORUM_CATEGORY_ID}
    `;
  }

  async #writeRelational(sql: WriteSQL, seq: number): Promise<void> {
    const accountIndex = seq % REL_ACCOUNT_COUNT;
    const contactIndex = seq % REL_CONTACTS_PER_ACCOUNT;
    const accountID = `rel-account-${accountIndex}`;
    const contactID = `${accountID}-contact-${contactIndex}`;
    await sql`
      INSERT INTO zero_throughput_rel_activity
        (id, org_id, account_id, contact_id, kind, body, seq)
      VALUES
        (
          ${`${this.#config.runID}-rel-activity-${seq}`},
          ${REL_ORG_ID},
          ${accountID},
          ${contactID},
          ${seq % 5 === 0 ? 'meeting' : 'note'},
          ${this.#payload},
          ${seq}
        )
    `;
    await sql`
      UPDATE zero_throughput_rel_account
      SET
        seq = ${seq},
        written_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = ${accountID}
    `;
    await sql`
      UPDATE zero_throughput_rel_org
      SET
        seq = ${seq},
        written_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = ${REL_ORG_ID}
    `;
  }
}
