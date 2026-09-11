import type {LogContext} from '@rocicorp/logger';
import type {Census} from './census.ts';
import type {SimClock} from './clock.ts';
import type {Incarnation} from './incarnation.ts';
import type {Oracles} from './oracles.ts';
import type {SimBackup} from './sim-backup.ts';
import type {SimPG} from './sim-pg.ts';
import type {RunConfig} from './steps.ts';
import type {Trace} from './trace.ts';
import type {SimNetwork, SubscribeOrder} from './transport.ts';

/** What a simulated node reaches of the run it is part of. */
export interface SimContext {
  readonly config: RunConfig;
  readonly runDir: string;
  /** The simulator's own LogContext, for handles it opens itself. */
  readonly lc: LogContext;
  readonly trace: Trace;
  readonly clock: SimClock;
  readonly census: Census;
  readonly pg: SimPG;
  readonly backup: SimBackup;
  readonly network: SimNetwork;
  readonly oracles: Oracles;

  /** When a `subscribe()` resolves; drawn from the step's seeded randomness. */
  subscribeOrder(): SubscribeOrder;

  /** The purge scheduler's yield between batches; see `pumpPurge`. */
  purgeYield(): Promise<void>;

  /** Records a failure, which the check after the step reports. */
  fail(message: string): void;

  /** A replication-manager incarnation, and its snapshot reservations, died. */
  reservationsLost(server: Incarnation): void;

  /** The replica file of replication-manager incarnation `server`, while it runs. */
  rmReplicaFile(server: Incarnation): string | undefined;

  /**
   * Watches a long-running `run()` of `incarnation`'s code. Rejecting fails
   * the run. Resolving fails it too, unless `onStop` takes the stop as
   * expected. Nothing a fenced incarnation's code does is reported.
   */
  watch(
    incarnation: Incarnation,
    name: string,
    running: Promise<unknown>,
    onStop?: () => void,
  ): void;
}
