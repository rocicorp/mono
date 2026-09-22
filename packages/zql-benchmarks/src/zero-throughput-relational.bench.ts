/**
 * Isolated zqlite push benchmark for the zero-throughput relational profile.
 *
 * This mirrors the E2E relational profile shape without zero-cache, CVR flush,
 * WebSockets, or client groups:
 *
 *   - 100 users
 *   - 3 standing query shapes per user
 *   - 50 rows per top-level query window
 *   - one logical write = add relActivity + edit relAccount + edit relOrg
 *
 * Run with:
 *   pnpm --filter zql-benchmarks run bench:mem zero-throughput-relational
 */

import {bench, describe, use} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  type Source,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {QueryDelegateImpl as MemoryQueryDelegate} from '../../zql/src/query/test/query-delegate.ts';
import {Database} from '../../zqlite/src/db.ts';
import {QueryDelegateImpl as ZqliteQueryDelegate} from '../../zqlite/src/query-delegate.ts';

const USERS = 100;
const QUERIES_PER_USER = 3;
const ROWS_PER_QUERY = 50;
const QUERY_INSTANCES = USERS * QUERIES_PER_USER;

const REL_ORG_ID = 'rel-org-0';
const REL_ACCOUNT_COUNT = 64;
const REL_CONTACTS_PER_ACCOUNT = 4;
const INITIAL_ACTIVITY_COUNT = 512;
const PAYLOAD = 'x'.repeat(256);

const relOrg = table('relOrg')
  .columns({
    id: string(),
    name: string(),
    region: string(),
    seq: number(),
    writtenAt: number(),
    updatedAt: number(),
  })
  .primaryKey('id');

const relAccount = table('relAccount')
  .columns({
    id: string(),
    orgID: string(),
    ownerID: string(),
    name: string(),
    status: string(),
    seq: number(),
    writtenAt: number(),
    updatedAt: number(),
  })
  .primaryKey('id');

const relContact = table('relContact')
  .columns({
    id: string(),
    accountID: string(),
    name: string(),
    role: string(),
    seq: number(),
    writtenAt: number(),
    updatedAt: number(),
  })
  .primaryKey('id');

const relActivity = table('relActivity')
  .columns({
    id: string(),
    orgID: string(),
    accountID: string(),
    contactID: string(),
    kind: string(),
    body: string(),
    seq: number(),
    writtenAt: number(),
    updatedAt: number(),
  })
  .primaryKey('id');

const relOrgRelationships = relationships(relOrg, ({many}) => ({
  accounts: many({
    sourceField: ['id'],
    destField: ['orgID'],
    destSchema: relAccount,
  }),
  activities: many({
    sourceField: ['id'],
    destField: ['orgID'],
    destSchema: relActivity,
  }),
}));

const relAccountRelationships = relationships(relAccount, ({many, one}) => ({
  org: one({
    sourceField: ['orgID'],
    destField: ['id'],
    destSchema: relOrg,
  }),
  contacts: many({
    sourceField: ['id'],
    destField: ['accountID'],
    destSchema: relContact,
  }),
  activities: many({
    sourceField: ['id'],
    destField: ['accountID'],
    destSchema: relActivity,
  }),
}));

const relContactRelationships = relationships(relContact, ({many, one}) => ({
  account: one({
    sourceField: ['accountID'],
    destField: ['id'],
    destSchema: relAccount,
  }),
  activities: many({
    sourceField: ['id'],
    destField: ['contactID'],
    destSchema: relActivity,
  }),
}));

const relActivityRelationships = relationships(relActivity, ({one}) => ({
  org: one({
    sourceField: ['orgID'],
    destField: ['id'],
    destSchema: relOrg,
  }),
  account: one({
    sourceField: ['accountID'],
    destField: ['id'],
    destSchema: relAccount,
  }),
  contact: one({
    sourceField: ['contactID'],
    destField: ['id'],
    destSchema: relContact,
  }),
}));

const schema = createSchema({
  tables: [relOrg, relAccount, relContact, relActivity],
  relationships: [
    relOrgRelationships,
    relAccountRelationships,
    relContactRelationships,
    relActivityRelationships,
  ],
  enableLegacyMutators: false,
  enableLegacyQueries: false,
});

const builder = createBuilder(schema);

type TableName = keyof typeof schema.tables;
type RelationalQuery = Query<TableName, typeof schema>;
type MutableRow = Record<string, string | number>;
type FlushableView = {
  readonly data: unknown;
  destroy(): void;
  flush(): void;
};

type SourceSet = {
  readonly relActivity: Source;
  readonly relAccount: Source;
  readonly relOrg: Source;
};

type BenchState = {
  org: MutableRow;
  readonly views: readonly FlushableView[];
  readonly accounts: MutableRow[];
  readonly sources: SourceSet;
  readonly close: () => void;
  nextSeq: number;
};

type SeedData = {
  readonly org: MutableRow;
  readonly accounts: MutableRow[];
  readonly contacts: readonly Row[];
  readonly activities: readonly Row[];
  readonly nextSeq: number;
};

function buildRelationalQuery(queryIndex: number): RelationalQuery {
  switch (queryIndex % QUERIES_PER_USER) {
    case 0:
      return builder.relOrg
        .where('id', REL_ORG_ID)
        .related('accounts', q =>
          q
            .orderBy('seq', 'desc')
            .limit(ROWS_PER_QUERY)
            .related('contacts')
            .related('activities', a =>
              a.orderBy('seq', 'desc').limit(5).related('contact'),
            ),
        )
        .related('activities', q =>
          q.orderBy('seq', 'desc').limit(ROWS_PER_QUERY),
        );

    case 1:
      return builder.relAccount
        .where('orgID', REL_ORG_ID)
        .related('org')
        .related('contacts')
        .related('activities', q =>
          q.orderBy('seq', 'desc').limit(5).related('contact'),
        )
        .orderBy('seq', 'desc')
        .limit(ROWS_PER_QUERY);

    case 2:
      return builder.relActivity
        .where('orgID', REL_ORG_ID)
        .related('org')
        .related('account', q => q.related('contacts'))
        .related('contact')
        .orderBy('seq', 'desc')
        .limit(ROWS_PER_QUERY);
  }
  throw new Error(`Invalid relational query index: ${queryIndex}`);
}

function createDB(): Database {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.unsafeMode(true);
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.pragma('foreign_keys = OFF');
  db.exec(`
    CREATE TABLE relOrg (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      region TEXT NOT NULL,
      seq REAL NOT NULL,
      writtenAt REAL NOT NULL,
      updatedAt REAL NOT NULL
    );

    CREATE TABLE relAccount (
      id TEXT PRIMARY KEY,
      orgID TEXT NOT NULL,
      ownerID TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      seq REAL NOT NULL,
      writtenAt REAL NOT NULL,
      updatedAt REAL NOT NULL
    );
    CREATE INDEX relAccount_org_seq_idx
      ON relAccount (orgID, seq DESC, id ASC);

    CREATE TABLE relContact (
      id TEXT PRIMARY KEY,
      accountID TEXT NOT NULL,
      name TEXT NOT NULL,
      role TEXT NOT NULL,
      seq REAL NOT NULL,
      writtenAt REAL NOT NULL,
      updatedAt REAL NOT NULL
    );
    CREATE INDEX relContact_account_idx
      ON relContact (accountID, id ASC);

    CREATE TABLE relActivity (
      id TEXT PRIMARY KEY,
      orgID TEXT NOT NULL,
      accountID TEXT NOT NULL,
      contactID TEXT NOT NULL,
      kind TEXT NOT NULL,
      body TEXT NOT NULL,
      seq REAL NOT NULL,
      writtenAt REAL NOT NULL,
      updatedAt REAL NOT NULL
    );
    CREATE INDEX relActivity_org_seq_idx
      ON relActivity (orgID, seq DESC, id ASC);
    CREATE INDEX relActivity_account_seq_idx
      ON relActivity (accountID, seq DESC, id ASC);
    CREATE INDEX relActivity_contact_seq_idx
      ON relActivity (contactID, seq DESC, id ASC);
  `);
  return db;
}

function makeSeedData(): SeedData {
  const org: MutableRow = {
    id: REL_ORG_ID,
    name: 'Throughput Org',
    region: 'na',
    seq: 0,
    writtenAt: 0,
    updatedAt: 0,
  };
  const accounts: MutableRow[] = Array.from(
    {length: REL_ACCOUNT_COUNT},
    (_, index) => ({
      id: `rel-account-${index}`,
      orgID: REL_ORG_ID,
      ownerID: `owner-${index % 8}`,
      name: `Account ${index}`,
      status: index % 3 === 0 ? 'risk' : 'active',
      seq: 0,
      writtenAt: 0,
      updatedAt: 0,
    }),
  );
  const contacts: Row[] = accounts.flatMap(account =>
    Array.from({length: REL_CONTACTS_PER_ACCOUNT}, (_, index) => ({
      id: `${account.id}-contact-${index}`,
      accountID: account.id,
      name: `Contact ${index} at ${account.name}`,
      role: index === 0 ? 'buyer' : 'stakeholder',
      seq: 0,
      writtenAt: 0,
      updatedAt: 0,
    })),
  );
  const activities: Row[] = [];

  for (let seq = 1; seq <= INITIAL_ACTIVITY_COUNT; seq++) {
    const accountIndex = seq % REL_ACCOUNT_COUNT;
    const account = accounts[accountIndex];
    account.seq = seq;
    account.writtenAt = seq;
    account.updatedAt = seq;
    org.seq = seq;
    org.writtenAt = seq;
    org.updatedAt = seq;
    activities.push(makeActivityRow(seq));
  }

  return {
    org,
    accounts,
    contacts,
    activities,
    nextSeq: INITIAL_ACTIVITY_COUNT + 1,
  };
}

function seedDB(db: Database, seed: SeedData): void {
  const insertOrg = db.prepare(`
    INSERT INTO relOrg
      (id, name, region, seq, writtenAt, updatedAt)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const insertAccount = db.prepare(`
    INSERT INTO relAccount
      (id, orgID, ownerID, name, status, seq, writtenAt, updatedAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertContact = db.prepare(`
    INSERT INTO relContact
      (id, accountID, name, role, seq, writtenAt, updatedAt)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const insertActivity = db.prepare(`
    INSERT INTO relActivity
      (id, orgID, accountID, contactID, kind, body, seq, writtenAt, updatedAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  db.transaction(() => {
    insertOrg.run(
      seed.org.id,
      seed.org.name,
      seed.org.region,
      seed.org.seq,
      seed.org.writtenAt,
      seed.org.updatedAt,
    );
    for (const account of seed.accounts) {
      insertAccount.run(
        account.id,
        account.orgID,
        account.ownerID,
        account.name,
        account.status,
        account.seq,
        account.writtenAt,
        account.updatedAt,
      );
    }
    for (const contact of seed.contacts) {
      insertContact.run(
        contact.id,
        contact.accountID,
        contact.name,
        contact.role,
        contact.seq,
        contact.writtenAt,
        contact.updatedAt,
      );
    }
    for (const activity of seed.activities) {
      insertActivity.run(
        activity.id,
        activity.orgID,
        activity.accountID,
        activity.contactID,
        activity.kind,
        activity.body,
        activity.seq,
        activity.writtenAt,
        activity.updatedAt,
      );
    }
  });
}

function setupZqlite(): BenchState {
  const db = createDB();
  const seed = makeSeedData();
  seedDB(db, seed);
  const delegate = new ZqliteQueryDelegate(
    createSilentLogContext(),
    db,
    schema,
    {
      format: 'text',
      level: 'error',
      ivmSampling: 0,
      slowHydrateThreshold: 0,
      slowRowThreshold: 0,
    },
  );
  const queries = Array.from({length: QUERIES_PER_USER}, (_, queryIndex) =>
    buildRelationalQuery(queryIndex),
  );
  const views: FlushableView[] = [];
  for (let user = 0; user < USERS; user++) {
    for (const query of queries) {
      views.push(delegate.materialize(query) as unknown as FlushableView);
    }
  }
  use(views.length);
  return {
    org: seed.org,
    views,
    accounts: seed.accounts,
    sources: {
      relActivity: delegate.getSource('relActivity'),
      relAccount: delegate.getSource('relAccount'),
      relOrg: delegate.getSource('relOrg'),
    },
    close: () => db.close(),
    nextSeq: seed.nextSeq,
  };
}

function setupMemory(): BenchState {
  const seed = makeSeedData();
  const sources = Object.fromEntries(
    Object.entries(schema.tables).map(([name, tableSchema]) => [
      name,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  ) as Record<TableName, MemorySource>;

  addToMemorySource(sources.relOrg, seed.org);
  for (const account of seed.accounts) {
    addToMemorySource(sources.relAccount, account);
  }
  for (const contact of seed.contacts) {
    addToMemorySource(sources.relContact, contact);
  }
  for (const activity of seed.activities) {
    addToMemorySource(sources.relActivity, activity);
  }

  const delegate = new MemoryQueryDelegate({sources});
  const queries = Array.from({length: QUERIES_PER_USER}, (_, queryIndex) =>
    buildRelationalQuery(queryIndex),
  );
  const views: FlushableView[] = [];
  for (let user = 0; user < USERS; user++) {
    for (const query of queries) {
      views.push(delegate.materialize(query) as unknown as FlushableView);
    }
  }
  use(views.length);
  return {
    org: seed.org,
    views,
    accounts: seed.accounts,
    sources: {
      relActivity: sources.relActivity,
      relAccount: sources.relAccount,
      relOrg: sources.relOrg,
    },
    close: () => {},
    nextSeq: seed.nextSeq,
  };
}

function addToMemorySource(source: MemorySource, row: Row): void {
  consume(source.push(makeSourceChangeAdd({...row})));
}

function teardown(state: BenchState): void {
  for (const view of state.views) {
    view.destroy();
  }
  state.close();
}

function pushLogicalWrite(state: BenchState): void {
  const seq = state.nextSeq++;
  const accountIndex = seq % REL_ACCOUNT_COUNT;

  consume(
    state.sources.relActivity.push(makeSourceChangeAdd(makeActivityRow(seq))),
  );

  const oldAccount = state.accounts[accountIndex];
  const newAccount = {
    ...oldAccount,
    seq,
    writtenAt: seq,
    updatedAt: seq,
  };
  consume(
    state.sources.relAccount.push(makeSourceChangeEdit(newAccount, oldAccount)),
  );
  state.accounts[accountIndex] = newAccount;

  const oldOrg = state.org;
  const newOrg = {
    ...oldOrg,
    seq,
    writtenAt: seq,
    updatedAt: seq,
  };
  consume(state.sources.relOrg.push(makeSourceChangeEdit(newOrg, oldOrg)));
  state.org = newOrg;
}

function flushViews(state: BenchState): void {
  for (const view of state.views) {
    view.flush();
  }
}

function makeActivityRow(seq: number): Row {
  const accountIndex = seq % REL_ACCOUNT_COUNT;
  const contactIndex = seq % REL_CONTACTS_PER_ACCOUNT;
  const accountID = `rel-account-${accountIndex}`;
  return {
    id: `bench-rel-activity-${seq}`,
    orgID: REL_ORG_ID,
    accountID,
    contactID: `${accountID}-contact-${contactIndex}`,
    kind: seq % 5 === 0 ? 'meeting' : 'note',
    body: PAYLOAD,
    seq,
    writtenAt: seq,
    updatedAt: seq,
  };
}

const pushOptions = {
  max_samples: 1_000,
};

describe('zero-throughput relational zqlite push', () => {
  bench(
    `${QUERY_INSTANCES} queries: logical write push only`,
    function* () {
      const state = setupZqlite();

      yield () => {
        pushLogicalWrite(state);
      };

      teardown(state);
    },
    pushOptions,
  );

  bench(
    `${QUERY_INSTANCES} queries: logical write push + flush views`,
    function* () {
      const state = setupZqlite();

      yield () => {
        pushLogicalWrite(state);
        flushViews(state);
      };

      teardown(state);
    },
    pushOptions,
  );
});

describe('zero-throughput relational memory push', () => {
  bench(
    `${QUERY_INSTANCES} queries: logical write push only`,
    function* () {
      const state = setupMemory();

      yield () => {
        pushLogicalWrite(state);
      };

      teardown(state);
    },
    pushOptions,
  );

  bench(
    `${QUERY_INSTANCES} queries: logical write push + flush views`,
    function* () {
      const state = setupMemory();

      yield () => {
        pushLogicalWrite(state);
        flushViews(state);
      };

      teardown(state);
    },
    pushOptions,
  );
});
