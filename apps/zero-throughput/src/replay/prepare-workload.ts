/**
 * Builds a replay workload from a CVR export: the client groups that were
 * live at the snapshot, the named queries each one held, and the screen
 * queries sessions register while they navigate.
 *
 * The export is one directory per shard holding `instances.csv.gz`,
 * `desires.csv.gz` and `queries.csv.gz` in Postgres `COPY ... CSV HEADER`
 * form. The output names real users, so keep it out of the repository.
 */
import '../../../../packages/shared/src/dotenv.ts';

import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {dirname, join, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import type {ReadonlyJSONValue} from '../../../../packages/shared/src/json.ts';
import {getOrInsertComputed} from '../../../../packages/shared/src/map.ts';
import {parseOptions} from '../../../../packages/shared/src/options.ts';
import * as v from '../../../../packages/shared/src/valita.ts';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import {log, percentile} from '../util.ts';
import {readCSV} from './csv.ts';
import {
  classifyQueryNames,
  inferUserID,
  parseTimestamp,
  type ReplayWorkload,
  type ScreenQuery,
  type WorkloadGroup,
  type WorkloadNameStats,
  type WorkloadQuery,
} from './workload.ts';

const options = {
  cvrDir: v.string(),
  out: v.string(),
  /** ISO time of the snapshot; defaults to the newest `lastActive`. */
  snapshot: v.string().optional(),
  /** A group is live when it was active this recently before the snapshot. */
  liveWindowSeconds: v.number().default(300),
  /** Names held by at least this fraction of live groups are session queries. */
  sessionThreshold: v.number().default(0.5),
  /** A JSON array of names that are always session queries. */
  sessionNamesFile: v.string().optional(),
  userIDKeys: v.array(v.string()).default(['userId', 'userID', 'user_id']),
};

type LiveGroup = {
  readonly grantedAtMs: number | undefined;
  readonly clientSchemaHash: string | undefined;
};

type Desire = {
  readonly queryHash: string;
  readonly ttlMs: number;
  readonly patchVersion: string;
  readonly active: boolean;
};

const DEFAULT_TTL_MS = 5 * 60 * 1000;

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const config = parseOptions(options, {
    argv: argv[0] === '--' ? argv.slice(1) : argv,
    envNamePrefix: 'REPLAY_PREPARE_',
  });
  const cvrDir = resolve(config.cvrDir);
  const sessionNames = new Set<string>(
    config.sessionNamesFile === undefined
      ? []
      : (JSON.parse(
          await readFile(config.sessionNamesFile, 'utf8'),
        ) as string[]),
  );

  const snapshotMs =
    config.snapshot === undefined
      ? await newestLastActive(cvrDir)
      : Date.parse(config.snapshot);
  if (!Number.isFinite(snapshotMs)) {
    throw new Error(`Invalid snapshot time: ${String(config.snapshot)}`);
  }
  const liveSinceMs = snapshotMs - config.liveWindowSeconds * 1000;
  log(`snapshot ${new Date(snapshotMs).toISOString()}`);

  // 1. Live groups, their client schemas, and the recent connect rate.
  const live = new Map<string, LiveGroup>();
  const schemaCounts = new Map<string, number>();
  const schemaText = new Map<string, string>();
  let connectsLastHour = 0;
  for await (const row of readCSV(join(cvrDir, 'instances.csv.gz'))) {
    const grantedAtMs = parseTimestamp(row.grantedAt);
    if (
      grantedAtMs !== undefined &&
      grantedAtMs <= snapshotMs &&
      grantedAtMs > snapshotMs - 3_600_000
    ) {
      connectsLastHour++;
    }
    const lastActiveMs = parseTimestamp(row.lastActive);
    if (
      row.deleted === 't' ||
      lastActiveMs === undefined ||
      lastActiveMs < liveSinceMs ||
      lastActiveMs > snapshotMs
    ) {
      continue;
    }
    let clientSchemaHash: string | undefined;
    if (row.clientSchema !== '') {
      clientSchemaHash = createHash('sha1')
        .update(row.clientSchema)
        .digest('hex');
      schemaCounts.set(
        clientSchemaHash,
        (schemaCounts.get(clientSchemaHash) ?? 0) + 1,
      );
      if (!schemaText.has(clientSchemaHash)) {
        schemaText.set(clientSchemaHash, row.clientSchema);
      }
    }
    live.set(row.clientGroupID, {grantedAtMs, clientSchemaHash});
  }
  log(`${live.size} live client groups`);
  const topSchema = [...schemaCounts].toSorted((a, b) => b[1] - a[1])[0];
  if (topSchema === undefined) {
    throw new Error('No live client group recorded a client schema');
  }
  log(
    `client schema ${topSchema[0].slice(0, 10)} is used by ${topSchema[1]} of ` +
      `${live.size} live groups (${schemaCounts.size} variants)`,
  );

  // 2. The desires of live groups.
  const desires = new Map<string, Desire[]>();
  for await (const row of readCSV(join(cvrDir, 'desires.csv.gz'))) {
    if (!live.has(row.clientGroupID) || row.deleted === 't') {
      continue;
    }
    const ttlMs = row.ttlMs === '' ? DEFAULT_TTL_MS : Number(row.ttlMs);
    getOrInsertComputed(desires, row.clientGroupID, newList).push({
      queryHash: row.queryHash,
      ttlMs: Number.isFinite(ttlMs) ? ttlMs : DEFAULT_TTL_MS,
      patchVersion: row.patchVersion,
      active: row.inactivatedAt === '',
    });
  }

  // 3. The names and arguments of the desired queries.
  const wanted = new Set<string>();
  for (const [cg, list] of desires) {
    for (const d of list) {
      wanted.add(`${cg}\u0000${d.queryHash}`);
    }
  }
  const namedQueries = new Map<
    string,
    {name: string; args: readonly ReadonlyJSONValue[]}
  >();
  for await (const row of readCSV(join(cvrDir, 'queries.csv.gz'))) {
    const key = `${row.clientGroupID}\u0000${row.queryHash}`;
    if (
      !wanted.has(key) ||
      row.internal === 't' ||
      row.deleted === 't' ||
      row.queryName === ''
    ) {
      continue;
    }
    namedQueries.set(key, {
      name: row.queryName,
      args:
        row.queryArgs === ''
          ? []
          : (JSON.parse(row.queryArgs) as ReadonlyJSONValue[]),
    });
  }

  // 4. Per-group active queries, classified by how widely they are held.
  type Held = WorkloadQuery & {readonly patchVersion: string};
  const active = new Map<string, Held[]>();
  const inactiveByGroup = new Map<string, WorkloadQuery[]>();
  const groupsHoldingName = new Map<string, number>();
  const desiresByName = new Map<string, number>();
  for (const [cg, list] of desires) {
    const names = new Set<string>();
    for (const d of list) {
      const q = namedQueries.get(`${cg}\u0000${d.queryHash}`);
      if (q === undefined) {
        continue;
      }
      const held = {
        name: q.name,
        args: q.args,
        ttlMs: d.ttlMs,
        patchVersion: d.patchVersion,
      };
      if (d.active) {
        getOrInsertComputed(active, cg, newList).push(held);
        names.add(q.name);
        desiresByName.set(q.name, (desiresByName.get(q.name) ?? 0) + 1);
      } else {
        getOrInsertComputed(inactiveByGroup, cg, newList).push(held);
      }
    }
    for (const name of names) {
      groupsHoldingName.set(name, (groupsHoldingName.get(name) ?? 0) + 1);
    }
  }
  const kinds = classifyQueryNames(
    groupsHoldingName,
    live.size,
    config.sessionThreshold,
    sessionNames,
  );

  const groups: WorkloadGroup[] = [];
  const screenPool = new Map<string, ScreenQuery>();
  let groupsWithoutUser = 0;
  let screenDesires = 0;
  let sessionMinutes = 0;
  for (const [cg, held] of active) {
    const userID = inferUserID(held, config.userIDKeys);
    if (userID === undefined) {
      groupsWithoutUser++;
      continue;
    }
    const sessionQueries = held
      .filter(q => kinds.get(q.name) === 'session')
      .sort(
        (a, b) =>
          compare(a.patchVersion, b.patchVersion) || compare(a.name, b.name),
      )
      .map(({name, args, ttlMs}) => ({name, args, ttlMs}));
    groups.push({sourceClientGroupID: cg, userID, sessionQueries});

    const screens = [
      ...held.filter(q => kinds.get(q.name) === 'screen'),
      ...(inactiveByGroup.get(cg) ?? []).filter(
        q => kinds.get(q.name) !== 'session',
      ),
    ];
    for (const q of screens) {
      const key = JSON.stringify([q.name, q.args]);
      const existing = screenPool.get(key);
      screenPool.set(key, {
        name: q.name,
        args: q.args,
        ttlMs: q.ttlMs,
        weight: (existing?.weight ?? 0) + 1,
      });
    }
    screenDesires += screens.length;
    const grantedAtMs = live.get(cg)?.grantedAtMs;
    if (grantedAtMs !== undefined) {
      sessionMinutes += Math.max(1, (snapshotMs - grantedAtMs) / 60_000);
    }
  }
  groups.sort((a, b) => compare(a.sourceClientGroupID, b.sourceClientGroupID));

  const names: WorkloadNameStats[] = Array.from(
    groupsHoldingName,
    ([name, n]) => ({
      name,
      groups: n,
      desires: desiresByName.get(name) ?? 0,
      kind: kinds.get(name) ?? ('screen' as const),
    }),
  ).sort((a, b) => b.groups - a.groups || compare(a.name, b.name));
  const sessionCounts = groups.map(g => g.sessionQueries.length);

  const workload: ReplayWorkload = {
    formatVersion: 1,
    source: {cvrDir, generatedAt: new Date().toISOString()},
    clientSchema: JSON.parse(
      schemaText.get(topSchema[0]) as string,
    ) as ClientSchema,
    groups,
    screenQueries: [...screenPool.values()].toSorted(
      (a, b) => b.weight - a.weight || compare(a.name, b.name),
    ),
    stats: {
      snapshot: new Date(snapshotMs).toISOString(),
      liveWindowSeconds: config.liveWindowSeconds,
      liveGroups: live.size,
      groupsWithoutUser,
      connectsLastHour,
      meanSessionSeconds:
        connectsLastHour === 0
          ? 0
          : Math.round((live.size / connectsLastHour) * 3600),
      screenQueriesPerSessionMinute:
        sessionMinutes === 0 ? 0 : screenDesires / sessionMinutes,
      sessionQueriesPerGroup: {
        p50: percentile(sessionCounts, 50),
        p90: percentile(sessionCounts, 90),
        max: Math.max(0, ...sessionCounts),
      },
      names,
    },
  };

  await mkdir(dirname(resolve(config.out)), {recursive: true});
  await writeFile(config.out, JSON.stringify(workload, null, 1));
  const s = workload.stats;
  log(
    `${groups.length} groups replayable (${groupsWithoutUser} without a user), ` +
      `${workload.screenQueries.length} distinct screen queries`,
  );
  log(
    `session queries per group p50=${s.sessionQueriesPerGroup.p50} ` +
      `p90=${s.sessionQueriesPerGroup.p90}; ${connectsLastHour} connects in ` +
      `the last hour -> mean session ${s.meanSessionSeconds}s; ` +
      `${s.screenQueriesPerSessionMinute.toFixed(3)} screen queries/session-minute`,
  );
  for (const n of names) {
    log(`  ${n.kind.padEnd(7)} ${String(n.groups).padStart(6)}  ${n.name}`);
  }
  log(`wrote ${config.out}`);
}

async function newestLastActive(cvrDir: string): Promise<number> {
  let newest = -Infinity;
  for await (const row of readCSV(join(cvrDir, 'instances.csv.gz'))) {
    const t = parseTimestamp(row.lastActive);
    if (t !== undefined && t > newest) {
      newest = t;
    }
  }
  return newest;
}

function newList<T>(): T[] {
  return [];
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  await main();
}
