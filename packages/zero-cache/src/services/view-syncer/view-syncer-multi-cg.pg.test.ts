import {beforeEach, describe, expect, vi} from 'vitest';
import {must} from '../../../../shared/src/must.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {type PgTest, test} from '../../test/db.ts';
import {computePipelineDedupStats} from '../../workers/syncer.ts';
import {
  expectEquivalentPokes,
  expectNoPokes,
  ISSUES_QUERY,
  messages,
  nextPoke,
  permissionsAll,
  pokedRowPatches,
  setupMultiCG,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {SyncContext} from './view-syncer.ts';

// Baseline coverage for the multi-client-group harness: N ViewSyncerServices
// over one replica, each independently advancing its own pipelines. These
// tests lock in today's per-client-group behavior as the equivalence baseline
// for shared pipeline advancement (see
// apps/zero-throughput/reports/2026-07-rm-vs-fanout/design-10x.md, lever A1):
// client groups running the same transformed query must observe the same row
// patches whether each advances its own pipeline (today) or a shared one.
describe('view-syncer/multi-cg', () => {
  const CG1 = 'cg-1';
  const CG2 = 'cg-2';
  const CG3 = 'cg-3';

  let harness: Awaited<ReturnType<typeof setupMultiCG>>;

  const ctx = (clientGroup: string): SyncContext => ({
    clientID: `foo-${clientGroup}`,
    profileID: 'p0000g00000003203',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    httpCookie: undefined,
    origin: undefined,
    userID: 'user-1',
    auth: undefined,
  });

  beforeEach<PgTest>(async ({testDBs}) => {
    harness = await setupMultiCG(
      testDBs,
      'view_syncer_multi_cg_test',
      permissionsAll,
      {clientGroupIDs: [CG1, CG2, CG3]},
    );

    return async () => {
      vi.useRealTimers();
      harness.clearMocks();
      await harness.stopAll();
      await testDBs.drop(harness.cvrDB, harness.upstreamDb);
      harness.replicaDbFile.delete();
    };
  });

  async function hydrateAll() {
    const {clientGroups} = harness;
    const cg1 = must(clientGroups.get(CG1));
    const cg2 = must(clientGroups.get(CG2));
    const cg3 = must(clientGroups.get(CG3));

    // cg1 and cg2 run the same query; cg3 runs a disjoint one.
    const q1 = cg1.connect(ctx(CG1), [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    const q2 = cg2.connect(ctx(CG2), [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    const q3 = cg3.connect(ctx(CG3), [
      {op: 'put', hash: 'users-query', ast: USERS_QUERY},
    ]);

    // The desired-queries config poke arrives on connect; hydration happens
    // once the replica is ready.
    await Promise.all([nextPoke(q1), nextPoke(q2), nextPoke(q3)]);
    harness.pushVersionReady();
    const [p1, p2, p3] = await Promise.all([
      nextPoke(q1),
      nextPoke(q2),
      nextPoke(q3),
    ]);
    return {q1, q2, q3, p1, p2, p3, cg1, cg2, cg3};
  }

  test('identical queries observe identical row patches; disjoint queries are isolated', async () => {
    const {q1, q2, q3, p1, p2, p3} = await hydrateAll();

    // Hydration: cg1 and cg2 see the same issue rows.
    const initialRows = expectEquivalentPokes([p1, p2]);
    expect(initialRows.length).toBeGreaterThan(0);
    expect(pokedRowPatches(p3).length).toBeGreaterThan(0);

    // An issues change pokes cg1 and cg2 identically, and not cg3.
    harness.advanceAll(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'updated title',
        owner: '100',
        big: 9007199254740991,
      }),
    );
    const [a1, a2] = await Promise.all([nextPoke(q1), nextPoke(q2)]);
    const advanceRows = expectEquivalentPokes([a1, a2]);
    expect(advanceRows.length).toBeGreaterThan(0);
    await expectNoPokes(q3);

    // A users change pokes cg3, and not cg1/cg2.
    harness.advanceAll(
      '124',
      messages.update('users', {id: '100', name: 'Alicia'}),
    );
    const u3 = await nextPoke(q3);
    expect(pokedRowPatches(u3).length).toBeGreaterThan(0);
    await expectNoPokes(q1);
    await expectNoPokes(q2);
  });

  test('multiple advances stay equivalent across client groups', async () => {
    const {q1, q2} = await hydrateAll();

    const collected: Downstream[][][] = [];
    for (const [watermark, title] of [
      ['123', 'one'],
      ['124', 'two'],
      ['125', 'three'],
    ] as const) {
      harness.advanceAll(
        watermark,
        messages.update('issues', {
          id: '2',
          title,
          owner: '101',
          big: -9007199254740991,
        }),
      );
      collected.push(await Promise.all([nextPoke(q1), nextPoke(q2)]));
    }
    for (const pokes of collected) {
      expectEquivalentPokes(pokes);
    }
  });

  test('pipeline dedup telemetry counts sharing across client groups', async () => {
    const {cg1, cg2, cg3} = await hydrateAll();

    const stats = computePipelineDedupStats([cg1.vs, cg2.vs, cg3.vs]);

    // ISSUES_QUERY is shared by cg1+cg2; USERS_QUERY is unique to cg3.
    expect(stats.clientTotal).toBe(3);
    expect(stats.clientHashes.size).toBe(2);
    expect(
      Array.from(stats.clientHashes.values(), ({count}) => count).sort(
        (a, b) => a - b,
      ),
    ).toEqual([1, 2]);

    // Internal queries (lmids, mutationResults) embed the clientGroupID in
    // their AST, so they never dedup across client groups.
    expect(stats.internalTotal).toBeGreaterThanOrEqual(3);
    expect(stats.internalUnique).toBe(stats.internalTotal);

    // All client groups use the same client schema.
    expect(stats.clientSchemas).toBe(1);
  });
});
