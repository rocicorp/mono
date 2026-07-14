/**
 * RM → ViewSyncer fan-out isolation harness.
 *
 * Spawns K protocol-faithful change-stream subscribers against a running
 * replication-manager (change-streamer) and measures how many changes/s the RM
 * can fan out as K grows — with no view-syncer IVM, CVR, or client work in the
 * way. This isolates the RM-side cost of the fan-out (per-subscriber ws.send +
 * per-message ACK) and the flow-control coupling (majority-ACK + consensus
 * padding) suspected of making added view-syncers degrade rather than scale
 * replication throughput.
 *
 * Each subscriber connects exactly like a serving replicator
 * (`ChangeStreamerHttpClient.subscribe`, the same code path as
 * incremental-sync.ts), consumes the downstream, and discards it. The stream's
 * per-message ACK is sent by `streamIn` when the consumer pulls the next
 * message, so an optional `--ack-delay-ms` on a subset of subscribers models a
 * slow view-syncer whose apply loop lags — the exact "one slow subscriber
 * stalls the majority" scenario governed by
 * ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS.
 *
 * The subscription watermark + replicaVersion are read from the RM's replica
 * file (read-only), so subscribers start near the head and measure steady-state
 * fan-out rather than a long catchup.
 *
 * Usage:
 *   node src/scripts/subscriber-sim.ts \
 *     --change-streamer-uri ws://127.0.0.1:4851/ \
 *     --change-db postgresql://user:password@127.0.0.1:6436/postgres \
 *     --replica-file /tmp/zt-rm-replica.db \
 *     --app-id zero_throughput \
 *     --subscribers 8 --slow-subscribers 0 --ack-delay-ms 0 \
 *     --duration-ms 30000
 */
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {Database} from '../../../zqlite/src/db.ts';
import {StatementRunner} from '../db/statements.ts';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.ts';
import {
  type Downstream,
  PROTOCOL_VERSION,
} from '../services/change-streamer/change-streamer.ts';
import {getSubscriptionState} from '../services/replicator/schema/replication-state.ts';
import type {ShardID} from '../types/shards.ts';
import type {Source} from '../types/streams.ts';

type SimArgs = {
  changeStreamerURI: string;
  changeDB: string;
  replicaFile: string;
  appID: string;
  shardNum: number;
  subscribers: number;
  slowSubscribers: number;
  ackDelayMs: number;
  durationMs: number;
  warmupMs: number;
};

const nowMs = () => performance.now();
const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

function parseArgs(argv: readonly string[]): SimArgs {
  const args = argv[0] === '--' ? argv.slice(1) : argv;
  const map = new Map<string, string>();
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      if (eq >= 0) {
        map.set(a.slice(2, eq), a.slice(eq + 1));
      } else {
        map.set(a.slice(2), args[++i] ?? '');
      }
    }
  }
  const str = (k: string, d?: string) => map.get(k) ?? d;
  const num = (k: string, d: number) => {
    const v = map.get(k);
    return v === undefined ? d : Number(v);
  };
  const changeStreamerURI = str('change-streamer-uri');
  if (!changeStreamerURI) {
    throw new Error('--change-streamer-uri is required');
  }
  const replicaFile = str('replica-file');
  if (!replicaFile) {
    throw new Error('--replica-file is required (RM replica for watermark)');
  }
  return {
    changeStreamerURI,
    changeDB: str(
      'change-db',
      'postgresql://user:password@127.0.0.1:6436/postgres',
    ) as string,
    replicaFile,
    appID: str('app-id', 'zero_throughput') as string,
    shardNum: num('shard-num', 0),
    subscribers: num('subscribers', 1),
    slowSubscribers: num('slow-subscribers', 0),
    ackDelayMs: num('ack-delay-ms', 0),
    durationMs: num('duration-ms', 30_000),
    warmupMs: num('warmup-ms', 3_000),
  };
}

type SubStats = {
  id: number;
  slow: boolean;
  changes: number;
  commits: number;
  begins: number;
  bytes: number;
};

function approxSize(message: Downstream): number {
  try {
    return JSON.stringify(message[1]).length;
  } catch {
    return 0;
  }
}

async function runSubscriber(
  lc: LogContext,
  args: SimArgs,
  shard: ShardID,
  sub: {replicaVersion: string; watermark: string},
  id: number,
  slow: boolean,
  stopAt: number,
  measureStart: number,
): Promise<SubStats> {
  const client = new ChangeStreamerHttpClient(
    lc.withContext('sub', id),
    shard,
    args.changeDB,
    args.changeStreamerURI,
  );
  const stats: SubStats = {
    id,
    slow,
    changes: 0,
    commits: 0,
    begins: 0,
    bytes: 0,
  };
  let source: Source<Downstream> | undefined;
  try {
    source = await client.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: `sim-${id}`,
      id: `sim-${id}`,
      mode: 'serving',
      watermark: sub.watermark,
      replicaVersion: sub.replicaVersion,
      initial: false,
    });
    for await (const message of source) {
      const tag = message[0];
      // Only count changes committed after warmup so warmup catchup is excluded.
      const measuring = nowMs() >= measureStart;
      switch (tag) {
        case 'begin':
          if (measuring) {
            stats.begins++;
          }
          break;
        case 'commit':
        case 'rollback':
          if (measuring) {
            stats.commits++;
          }
          break;
        case 'data':
          if (measuring) {
            stats.changes++;
            stats.bytes += approxSize(message);
          }
          break;
        default:
          break; // status / error / control
      }
      if (nowMs() >= stopAt) {
        break;
      }
      if (slow && args.ackDelayMs > 0) {
        // Delay the pull of the next message => delays this message's ACK,
        // modelling a view-syncer whose apply loop lags.
        await sleep(args.ackDelayMs);
      }
    }
  } catch (e) {
    lc.error?.(`subscriber ${id} error`, e);
  } finally {
    source?.cancel();
  }
  return stats;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const lc = new LogContext('error', {task: 'subscriber-sim'}, consoleLogSink);
  const shard: ShardID = {appID: args.appID, shardNum: args.shardNum};

  // Read subscription state (replicaVersion + head watermark) from the RM
  // replica so subscribers start near the head.
  using db = new Database(lc, args.replicaFile, {readonly: true});
  const sub = getSubscriptionState(new StatementRunner(db));

  process.stderr.write(
    `subscriber-sim: K=${args.subscribers} slow=${args.slowSubscribers} ` +
      `ackDelayMs=${args.ackDelayMs} replicaVersion=${sub.replicaVersion} ` +
      `watermark=${sub.watermark} duration=${args.durationMs}ms\n`,
  );

  const startedAt = nowMs();
  const measureStart = startedAt + args.warmupMs;
  const stopAt = measureStart + args.durationMs;

  const subs = Array.from({length: args.subscribers}, (_, i) =>
    runSubscriber(
      lc,
      args,
      shard,
      sub,
      i,
      i < args.slowSubscribers,
      stopAt,
      measureStart,
    ),
  );
  const results = await Promise.all(subs);

  const measuredSec = args.durationMs / 1000;
  let totalChanges = 0;
  let totalCommits = 0;
  let totalBytes = 0;
  const perSub = results.map(r => {
    totalChanges += r.changes;
    totalCommits += r.commits;
    totalBytes += r.bytes;
    return {
      id: r.id,
      slow: r.slow,
      changesPerSec: r.changes / measuredSec,
      commitsPerSec: r.commits / measuredSec,
      changes: r.changes,
    };
  });

  const summary = {
    subscribers: args.subscribers,
    slowSubscribers: args.slowSubscribers,
    ackDelayMs: args.ackDelayMs,
    durationMs: args.durationMs,
    // Aggregate delivered work across all subscribers.
    totalChangesPerSec: totalChanges / measuredSec,
    totalCommitsPerSec: totalCommits / measuredSec,
    totalMBPerSec: totalBytes / 1e6 / measuredSec,
    // The pipeline can only advance as fast as the SLOWEST subscriber, because
    // the RM flow-control waits for a majority + padding. Report the spread.
    minSubChangesPerSec: Math.min(...perSub.map(s => s.changesPerSec)),
    maxSubChangesPerSec: Math.max(...perSub.map(s => s.changesPerSec)),
    perSub,
  };

  process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
}

await main();
