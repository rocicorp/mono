/**
 * The client-group lane of the zero-cache fuzzer: several client groups on one
 * sync worker, which is configured like a production one (the view-syncers
 * share a `SnapshotRowCache` and plan queries with the query planner), stay
 * query-equivalent to PostgreSQL through the generated write stream, swaps of
 * a unique key within one transaction, client reconnects and view-syncer
 * restarts. The tests live in `chinook-zero-cache-fuzzer-groups.pg.test.ts`.
 *
 * The groups desire different but overlapping queries, so that they read the
 * same replica rows through the shared cache while their pipelines skip
 * different writes (a pipeline skips a row that none of its queries could
 * show, e.g. one pinned to another primary key). That is the setting of
 * #6647, where a group that skipped a write shared a stale read of a table
 * with a second unique key. The upstream `customer` table gets such a key
 * (`email`), and the swaps move an email from one customer to another:
 *
 * - `narrow` (created first, so it tends to advance first) pins every table
 *   to a primary key, including `customer` to one the swaps never edit;
 * - `wide` has the generated query corpus on one client and the write-fuzz
 *   queries plus unpinned `customer` queries on another;
 * - `mixed` has a seeded sample of both, plus a `customer` pinned by `email`.
 *
 * At seeded points in the write stream, a client disconnects and reconnects
 * with its last cookie as the base cookie, after 1-3 writes that end with a
 * removal:
 *
 * - `reconnect-shared`: one of `wide`'s clients, while the other stays
 *   connected; it also puts a new query in its `initConnection`. It must end
 *   up with the same store as the client that stayed connected.
 * - `reconnect-solo`: `narrow`'s only client, within its keepalive, so it
 *   reconnects to the same view-syncer.
 * - `restart`: `mixed`'s view-syncer is stopped while its client is away, and
 *   the client reconnects to a new one, which loads the CVR from the database
 *   (and then again, without writes, which must not change the store).
 *
 * After each reconnect, the client's queries must match PostgreSQL, and its
 * store must equal the store of a client of a new group that desires the same
 * queries: catching a client up must produce what hydrating does.
 *
 * Every write is followed by a barrier: an update of a row that every group
 * desires, so that every connected client receives a poke once the replica
 * has the write, whether or not the write changed the group's queries.
 *
 * The lane is deterministic in the seed (the query sample, the swap and event
 * schedule), except for how the view-syncers' advancements interleave.
 */
import {expect} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {
  cmpVersions,
  versionFromString,
} from '../../../zero-cache/src/services/view-syncer/schema/types.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {newStaticQuery} from '../../../zql/src/query/static-query.ts';
import '../helpers/comparePg.ts';
import {
  PROTOCOL_QUERY_CASES,
  PROTOCOL_WRITE_FUZZ_CASES,
  ProtocolFuzzerClient,
  type ProtocolQueryCase,
  type SyncGroup,
  type SyncWorker,
  type startZeroCacheReplica,
  applyWriteFuzzMutation,
  expectProtocolMatchesPG,
  hashFor,
  mutationDescription,
  queryForHash,
  L1_QUERY_CASES,
  WRITE_FUZZ_CASES,
} from './chinook-zero-cache-fuzzer.test.helpers.ts';
import {pkOf} from './fuzz/axes.ts';
import {miniData} from './fuzz/mini.ts';
import {type Mutation, queryTables} from './fuzz/push.ts';
import {rng, type Rng} from './fuzz/rng.ts';
import {builder, schema} from './schema.ts';

type Harness = Awaited<ReturnType<typeof startZeroCacheReplica>>;

/** Gives `customer` a second unique key, which the swaps move between rows. */
export const GROUPS_UPSTREAM_SETUP =
  'CREATE UNIQUE INDEX customer_email_key ON customer (email);';

/** The `mediaType` row every group desires, updated by each barrier. */
const BARRIER_MEDIA_TYPE_ID = 2;
const BARRIER: ProtocolQueryCase = {
  label: 'barrier',
  query: builder.mediaType.where('id', '=', BARRIER_MEDIA_TYPE_ID),
};

/** The customers whose emails the swaps exchange. */
const SWAPPED_CUSTOMERS = [1, 2] as const;
const UNSWAPPED_CUSTOMER = 3;

/** The most queries checked per group after a write (beyond focus queries). */
const STEP_CHECKS = 4;

type EventKind = 'reconnect-shared' | 'reconnect-solo' | 'restart';
const EVENT_KINDS: readonly EventKind[] = [
  'reconnect-shared',
  'reconnect-solo',
  'restart',
];

type Step =
  | {
      readonly kind: 'write';
      readonly label: string;
      readonly mutation: Mutation;
    }
  | {readonly kind: 'swap'};

type Event = {
  readonly kind: EventKind;
  /** The step before which the client disconnects. */
  readonly at: number;
  /** How many steps run while it is disconnected. */
  readonly writes: number;
};

type LaneGroup = {
  readonly group: SyncGroup;
  readonly clients: ProtocolFuzzerClient[];
  /** The queries desired by the group's clients. */
  readonly cases: ProtocolQueryCase[];
};

export type GroupsFuzzStats = {
  writes: number;
  swaps: number;
  events: Record<EventKind, number>;
  stepChecks: number;
  fullChecks: number;
  freshChecks: number;
  /** How many view-syncers served each group. */
  starts: Record<string, number>;
};

function query(table: string): AnyQuery {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  return newStaticQuery(schema, table as any) as AnyQuery;
}

function pinned(
  table: string,
  column: string,
  value: string | number,
): ProtocolQueryCase {
  return {
    label: `pinned|${table}.${column}=${value}`,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    query: (query(table) as any).where(column, '=', value) as AnyQuery,
  };
}

/**
 * A query per table pinned to the primary key of the row the four-phase
 * writes edit, and one pinned to a row they never edit. `customer` is pinned
 * away from the swapped customers only.
 */
function pinnedCases(): ProtocolQueryCase[] {
  const out: ProtocolQueryCase[] = [];
  for (const [table, rows] of Object.entries(miniData)) {
    const pk = pkOf(table);
    if (
      pk.length !== 1 ||
      rows.length < 2 ||
      table === 'customer' ||
      table === 'mediaType'
    ) {
      continue;
    }
    for (const row of [rows[0], must(rows.at(-1))]) {
      out.push(pinned(table, pk[0], row[pk[0]] as number));
    }
  }
  out.push(pinned('customer', 'id', UNSWAPPED_CUSTOMER), {
    label: `pinned|customer.id=${UNSWAPPED_CUSTOMER}+supportRep`,
    query: builder.customer
      .where('id', '=', UNSWAPPED_CUSTOMER)
      .related('supportRep'),
  });
  return out;
}

const CUSTOMER_CASES: readonly ProtocolQueryCase[] = [
  {
    label: 'customers-by-email',
    query: builder.customer.orderBy('email', 'asc'),
  },
  {label: 'invoices+customer', query: builder.invoice.related('customer')},
];

function tablesOf(q: AnyQuery): Set<string> {
  return queryTables(asQueryInternals(q).ast);
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? (e.stack ?? e.message) : String(e);
}

/**
 * Runs the client-group lane over `harness` (started with
 * {@link GROUPS_UPSTREAM_SETUP}), throwing on the first divergence.
 */
export async function checkClientGroupsFuzz(
  harness: Harness,
  seed: number,
  budget: number,
): Promise<GroupsFuzzStats> {
  const lane = new GroupsLane(
    harness,
    await harness.startSyncWorker({production: true}),
    seed,
    budget,
  );
  return await lane.run();
}

class GroupsLane {
  readonly #harness: Harness;
  readonly #worker: SyncWorker;
  readonly #seed: number;
  readonly #r: Rng;
  readonly #budget: number;
  readonly #narrow: LaneGroup;
  readonly #wide: LaneGroup;
  readonly #mixed: LaneGroup;
  readonly #groups: readonly LaneGroup[];
  readonly #log: string[] = [];
  readonly #stats: GroupsFuzzStats = {
    writes: 0,
    swaps: 0,
    events: {'reconnect-shared': 0, 'reconnect-solo': 0, 'restart': 0},
    stepChecks: 0,
    fullChecks: 0,
    freshChecks: 0,
    starts: {},
  };
  #watermark = '';
  #barriers = 0;
  #freshGroups = 0;

  constructor(
    harness: Harness,
    worker: SyncWorker,
    seed: number,
    budget: number,
  ) {
    this.#harness = harness;
    this.#worker = worker;
    this.#seed = seed;
    this.#r = rng(seed ^ 0x6a09e667);
    this.#budget = budget;

    // Created in this order so that `narrow`, which skips the most writes,
    // tends to advance (and fill the row cache) first.
    const group = (id: string, clientIDs: readonly string[]): LaneGroup => {
      const g = worker.startGroup(id);
      return {
        group: g,
        clients: clientIDs.map(c => new ProtocolFuzzerClient(g, c)),
        cases: [],
      };
    };
    this.#narrow = group('narrow', ['n1']);
    this.#wide = group('wide', ['w1', 'w2']);
    this.#mixed = group('mixed', ['m1']);
    this.#groups = [this.#narrow, this.#wide, this.#mixed];
  }

  get #writeCases() {
    return this.#budget > 1 ? WRITE_FUZZ_CASES : PROTOCOL_WRITE_FUZZ_CASES;
  }

  async run(): Promise<GroupsFuzzStats> {
    try {
      await this.#start();
      await this.#runSteps();
      for (const g of this.#groups) {
        await this.#fullCheck(g, 'the end of the write stream');
        await this.#freshCheck(g, 'the end of the write stream');
      }
    } catch (e) {
      throw new Error(
        `client-group lane (seed ${this.#seed}, budget ${this.#budget}) ` +
          `failed; last actions:\n  ${this.#log.slice(-12).join('\n  ')}\n\n` +
          errorMessage(e),
      );
    }
    for (const g of this.#groups) {
      this.#stats.starts[g.group.id] = g.group.starts;
    }
    return this.#stats;
  }

  #note(action: string) {
    this.#log.push(action);
  }

  async #start() {
    const writeCases = this.#writeCases.map(c => ({
      label: c.label,
      query: c.query,
    }));
    const sample = <T>(cases: readonly T[]) =>
      this.#r.shuffle(cases).slice(0, Math.ceil(cases.length / 2));
    const pins = pinnedCases();

    const desired = new Map<ProtocolFuzzerClient, ProtocolQueryCase[]>([
      [this.#narrow.clients[0], [...pins, BARRIER]],
      [this.#wide.clients[0], [...PROTOCOL_QUERY_CASES, BARRIER]],
      [this.#wide.clients[1], [...writeCases, ...CUSTOMER_CASES, BARRIER]],
      [
        this.#mixed.clients[0],
        [
          ...sample(PROTOCOL_QUERY_CASES),
          ...sample(writeCases),
          ...sample(pins),
          pinned('customer', 'email', 'ann@example.com'),
          BARRIER,
        ],
      ],
    ]);
    for (const g of this.#groups) {
      for (const client of g.clients) {
        const cases = must(desired.get(client));
        client.connect();
        await client.setQueries(cases, `${client.clientID} initial queries`);
        g.cases.push(...cases);
      }
    }
    this.#note('connected every client and hydrated its queries');
    await this.#barrier('initial hydration');
    for (const g of this.#groups) {
      await this.#fullCheck(g, 'initial hydration');
      await this.#freshCheck(g, 'initial hydration');
    }
  }

  #steps(): Step[] {
    // The swaps are the only writes to `customer`: the four-phase writes
    // would violate its unique email key.
    const steps: Step[] = this.#writeCases.flatMap(c =>
      c.mutations
        .filter(mutation => mutation.table !== 'customer')
        .map(mutation => ({kind: 'write' as const, label: c.label, mutation})),
    );
    // An even number of swaps leaves the emails where they started.
    for (let i = 0; i < 2 * this.#budget; i++) {
      steps.splice(this.#r.int(steps.length + 1), 0, {kind: 'swap'});
    }
    return steps;
  }

  /**
   * One event of each kind per budget unit, in a shuffled order, each within
   * its own segment of the steps so that events never overlap.
   *
   * A client is away for 1-3 steps that end with a removal, so that catching
   * it up has to delete a row as well as add and edit them. (A removal that
   * is added back before the client returns nets out to an edit.)
   */
  #events(steps: readonly Step[]): Event[] {
    const kinds = this.#r.shuffle(
      EVENT_KINDS.flatMap(kind =>
        Array.from<EventKind>({length: this.#budget}).fill(kind),
      ),
    );
    const segment = Math.floor(steps.length / kinds.length);
    return kinds.map((kind, i) => {
      const first = i * segment;
      const removals = steps
        .map((step, end) => ({step, end}))
        .filter(
          ({step, end}) =>
            end >= first + 2 &&
            end < first + segment &&
            step.kind === 'write' &&
            step.mutation.kind === 'remove',
        );
      const {end} = must(
        this.#r.choose(removals),
        `no removal in steps [${first}, ${first + segment})`,
      );
      const writes = 1 + this.#r.int(3);
      return {kind, at: end - writes + 1, writes};
    });
  }

  async #runSteps() {
    const steps = this.#steps();
    const events = this.#events(steps);
    let active: Event | undefined;
    for (let i = 0; i < steps.length; i++) {
      const event = events.find(e => e.at === i);
      if (event) {
        await this.#disconnect(event);
        active = event;
      }
      await this.#step(steps[i], i);
      if (active && i === active.at + active.writes - 1) {
        await this.#reconnect(active);
        active = undefined;
      }
    }
    for (const kind of EVENT_KINDS) {
      expect(this.#stats.events[kind], `every scheduled ${kind} must run`).toBe(
        this.#budget,
      );
    }
  }

  async #step(step: Step, i: number) {
    if (step.kind === 'swap') {
      const description = `step ${i}: swap customer emails`;
      this.#note(description);
      await swapCustomerEmails(this.#harness.upstream);
      this.#stats.swaps++;
      await this.#barrier(description);
      await this.#stepChecks(description, ['customer'], CUSTOMER_CASES);
      return;
    }
    const description = `step ${i}: ${step.label}: ${mutationDescription(step.mutation)}`;
    this.#note(description);
    await applyWriteFuzzMutation(this.#harness.upstream, step.mutation);
    this.#stats.writes++;
    await this.#barrier(description);
    const focus = this.#writeCases.filter(c => c.label === step.label);
    await this.#stepChecks(description, [step.mutation.table], focus);
  }

  /**
   * Updates the barrier row and waits until every connected client has a
   * poke at or beyond the replica version that has it. Every earlier write
   * is then in every connected client's store.
   */
  async #barrier(description: string) {
    const baseline = await this.#harness.watermark();
    const result = await this.#harness.upstream`
      UPDATE media_type SET name = ${`barrier-${++this.#barriers}`}
       WHERE media_type_id = ${BARRIER_MEDIA_TYPE_ID}
      RETURNING 1`;
    expect(result.length).toBe(1);
    const state = await this.#harness.waitForReplicaVersion(
      `barrier after ${description}`,
      baseline,
    );
    this.#watermark = must(state.watermark, 'missing replica watermark');
    for (const g of this.#groups) {
      for (const client of g.clients) {
        if (client.connected) {
          await client.waitForCookieAtOrBeyond(this.#watermark, description);
        }
      }
    }
  }

  async #stepChecks(
    description: string,
    tables: readonly string[],
    focus: readonly ProtocolQueryCase[],
  ) {
    for (const g of this.#groups) {
      const client = g.clients.find(c => c.connected);
      if (!client) {
        continue;
      }
      const focused = g.cases.filter(c => focus.some(f => f.query === c.query));
      const touching = g.cases.filter(
        c =>
          !focused.includes(c) &&
          [...tablesOf(c.query)].some(t => tables.includes(t)),
      );
      const cases = [
        ...focused,
        ...this.#r.shuffle(touching).slice(0, STEP_CHECKS),
      ];
      await this.#check(g, client, cases, description);
      this.#stats.stepChecks += cases.length;
    }
  }

  async #fullCheck(g: LaneGroup, description: string) {
    const client = must(g.clients.find(c => c.connected));
    await this.#check(g, client, g.cases, description);
    this.#stats.fullChecks++;
  }

  async #check(
    g: LaneGroup,
    client: ProtocolFuzzerClient,
    cases: readonly ProtocolQueryCase[],
    description: string,
  ) {
    for (const c of cases) {
      try {
        await expectProtocolMatchesPG({
          ...this.#harness,
          client,
          query: c.query,
        });
      } catch (e) {
        // The matcher's message does not show the rows.
        const rows = JSON.stringify({
          client: await client.run(c.query),
          pg: await this.#harness.pg.run(c.query),
        });
        throw new Error(
          `group ${g.group.id} (via ${client.clientID}) diverged from ` +
            `PostgreSQL for ${c.label} after ${description}\n` +
            `${rows.slice(0, 4000)}\n${errorMessage(e)}`,
        );
      }
    }
  }

  /**
   * The store of `g`'s client must equal the store of a client of a new
   * group that desires the queries `g` has got: the rows a group syncs are
   * determined by its queries, however it got to them.
   */
  async #freshCheck(g: LaneGroup, description: string) {
    const client = must(g.clients.find(c => c.connected));
    const fresh = this.#worker.startGroup(
      `${g.group.id}-fresh${++this.#freshGroups}`,
    );
    const probe = new ProtocolFuzzerClient(fresh, 'probe');
    probe.connect();
    try {
      await probe.setQueries(
        Array.from(client.gotQueries, hash => ({
          label: hash,
          query: queryForHash(hash),
        })),
        `fresh ${g.group.id} queries`,
      );
      await probe.waitForCookieAtOrBeyond(this.#watermark, description);
      expect(
        client.rows(),
        `group ${g.group.id} (via ${client.clientID}) has a different ` +
          `store than a new group with the same queries after ${description}`,
      ).toEqual(probe.rows());
      this.#stats.freshChecks++;
    } finally {
      probe.disconnect();
      await fresh.stop();
    }
  }

  async #disconnect(event: Event) {
    this.#note(`${event.kind}: disconnect for ${event.writes} write(s)`);
    switch (event.kind) {
      case 'reconnect-shared':
        this.#wide.clients[0].disconnect();
        break;
      case 'reconnect-solo':
        // Reconnects within the keepalive, so to the same view-syncer.
        this.#narrow.group.keepalive();
        this.#narrow.clients[0].disconnect();
        break;
      case 'restart':
        this.#mixed.clients[0].disconnect();
        await this.#mixed.group.stop();
        break;
    }
  }

  async #reconnect(event: Event) {
    const description = `${event.kind} reconnect`;
    this.#note(description);
    switch (event.kind) {
      case 'reconnect-shared': {
        const [client, other] = this.#wide.clients;
        const added = this.#r.choose(
          L1_QUERY_CASES.cases.filter(
            c => !this.#wide.cases.some(w => w.query === c.query),
          ),
        );
        const put = added ? [added] : [];
        client.connect(put);
        this.#wide.cases.push(...put);
        await client.waitForGotQueries(
          put.map(c => hashFor(c.query)),
          description,
        );
        await client.waitForCookieAtOrBeyond(this.#watermark, description);
        await drainToSameCookie([client, other], description);
        expect(
          client.rows(),
          `${client.clientID} caught up to a different store than ` +
            `${other.clientID}, which stayed connected`,
        ).toEqual(other.rows());
        await this.#fullCheck(this.#wide, description);
        break;
      }
      case 'reconnect-solo': {
        const [client] = this.#narrow.clients;
        client.connect();
        await client.waitForCookieAtOrBeyond(this.#watermark, description);
        await this.#fullCheck(this.#narrow, description);
        await this.#freshCheck(this.#narrow, description);
        break;
      }
      case 'restart': {
        const [client] = this.#mixed.clients;
        this.#mixed.group.start();
        client.connect();
        await client.waitForCookieAtOrBeyond(this.#watermark, description);
        await this.#fullCheck(this.#mixed, description);
        await this.#freshCheck(this.#mixed, description);

        // Restart again without writes: the new view-syncer finds the CVR at
        // the replica's version, so it hydrates the queries without diffing
        // them against the CVR, and nothing may change.
        const before = client.rows();
        this.#note('restart without writes');
        client.disconnect();
        await this.#mixed.group.stop();
        this.#mixed.group.start();
        client.connect();
        await client.waitForCookieAtOrBeyond(this.#watermark, description);
        expect(
          client.rows(),
          `${client.clientID}'s store changed across a restart without writes`,
        ).toEqual(before);
        await this.#fullCheck(this.#mixed, `${description} without writes`);
        break;
      }
    }
    this.#stats.events[event.kind]++;
  }
}

/**
 * Exchanges the emails of the {@link SWAPPED_CUSTOMERS} in one transaction:
 * each statement frees an email that a later one takes, so the replica's
 * change log holds rows whose new values collide with the old values of
 * rows processed after them.
 */
async function swapCustomerEmails(upstream: PostgresDB) {
  const [a, b] = SWAPPED_CUSTOMERS;
  await upstream.begin(async tx => {
    const rows = await tx<{customerId: number; email: string}[]>`
      SELECT customer_id AS "customerId", email FROM customer
       WHERE customer_id IN (${a}, ${b})`;
    const email = (id: number) =>
      must(rows.find(r => Number(r.customerId) === id)).email;
    const [emailA, emailB] = [email(a), email(b)];
    await tx`UPDATE customer SET email = 'swap@example.com' WHERE customer_id = ${a}`;
    await tx`UPDATE customer SET email = ${emailA} WHERE customer_id = ${b}`;
    await tx`UPDATE customer SET email = ${emailB} WHERE customer_id = ${a}`;
  });
}

/** Drains `clients` (of one group) until they are all at the same cookie. */
async function drainToSameCookie(
  clients: readonly ProtocolFuzzerClient[],
  description: string,
) {
  for (let i = 0; i < 10; i++) {
    const cookies = clients.map(c => must(c.cookie));
    const max = cookies.reduce((a, b) =>
      cmpVersions(versionFromString(a), versionFromString(b)) >= 0 ? a : b,
    );
    if (cookies.every(c => c === max)) {
      return;
    }
    for (const client of clients) {
      await client.waitForCookie(max, description);
    }
  }
  throw new Error(`clients did not converge on a cookie for ${description}`);
}
