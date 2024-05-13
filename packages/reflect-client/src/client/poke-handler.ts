import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {Poke, PokeBody} from 'reflect-protocol';
import type {ClientID, Poke as ReplicachePoke} from 'replicache';
import type {MaybePromise} from 'shared/src/types.js';
import {assert} from 'shared/src/asserts.js';
import {BufferSizer} from 'shared/src/buffer-sizer.js';
import {mergePokes} from './merge-pokes.js';
import type {PresenceManager} from './presence-manager.js';

const BUFFER_SIZER_OPTIONS = {
  initialBufferSizeMs: 50,
  maxBufferSizeMs: 1000,
  minBufferSizeMs: -1000,
  adjustBufferSizeIntervalMs: 5_000,
} as const;
// TODO consider systems that don't run at 60fps (newer macs/ipads run RAF
// at 120fps).  Playback on 120fps systems will actually be 120fps with
// current logic, but the counting of missed frames will be incorrect (
// only counted as missed if its off by >= 2 frames on a 120fps system).
// This is not exactly 16 because raf does not fire exactly every 16 ms
// on 60fps systems, instead its usually in the range 14-18 ms (even
// when there is no interference from other JS).
const FRAME_INTERVAL_TOLERANCE_MS = 18;
export const RESET_PLAYBACK_OFFSET_THRESHOLD_MS = 1000;
const MAX_RECENT_POKE_LATENCIES_SIZE = 10;

type PendingPoke = Poke & {
  normalizedTimestamp?: number | undefined;
  playbackOffsetMs?: number | undefined;
  bufferNeededMs?: number | undefined;
  serverBufferMs?: number | undefined;
  receivedTimestamp: number;
};

export class PokeHandler {
  readonly #replicachePoke: (poke: ReplicachePoke) => Promise<void>;
  readonly #presenceManager: PresenceManager;
  readonly #onOutOfOrderPoke: () => MaybePromise<void>;
  readonly #clientID: ClientID;
  readonly #lc: LogContext;
  readonly #pokeBuffer: PendingPoke[] = [];
  readonly #bufferSizer: BufferSizer;
  readonly #maxRecentPokeLatenciesSize: number;
  #pokePlaybackLoopRunning = false;
  #lastRafPerfTimestamp = 0;
  #playbackOffsetMs: number | undefined = undefined;
  // Serializes calls to this._replicachePoke otherwise we can cause out of
  // order poke errors.
  readonly #pokeLock = new Lock();
  #timedPokeCount = 0;
  #missedTimedPokeCount = 0;
  #timedFrameCount = 0;
  #missedTimedFrameCount = 0;
  #timedPokeLatencyTotal = 0;
  readonly #recentPokeLatencies: number[] = [];

  constructor(
    replicachePoke: (poke: ReplicachePoke) => Promise<void>,
    presenceManager: PresenceManager,
    onOutOfOrderPoke: () => MaybePromise<void>,
    clientID: ClientID,
    lc: LogContext,
    bufferSizer = new BufferSizer(BUFFER_SIZER_OPTIONS),
    maxRecentPokeLatenciesSize = MAX_RECENT_POKE_LATENCIES_SIZE,
  ) {
    this.#replicachePoke = replicachePoke;
    this.#presenceManager = presenceManager;
    this.#onOutOfOrderPoke = onOutOfOrderPoke;
    this.#clientID = clientID;
    this.#lc = lc.withContext('PokeHandler');
    this.#bufferSizer = bufferSizer;
    this.#maxRecentPokeLatenciesSize = maxRecentPokeLatenciesSize;
  }

  handlePoke(pokeBody: PokeBody): number | undefined {
    const lc = this.#lc.withContext('requestID', pokeBody.requestID);
    lc.debug?.('Applying poke', pokeBody);
    if (pokeBody.debugServerBufferMs) {
      lc.debug?.('server buffer ms', pokeBody.debugServerBufferMs);
    }
    const now = Date.now();
    const thisClientID = this.#clientID;
    let lastMutationIDChangeForSelf: number | undefined;
    let bufferNeededMs = undefined;
    for (const poke of pokeBody.pokes) {
      const {timestamp} = poke;
      if (timestamp !== undefined) {
        const timestampOffset = now - timestamp;
        if (
          this.#playbackOffsetMs === undefined ||
          Math.abs(timestampOffset - this.#playbackOffsetMs) >
            RESET_PLAYBACK_OFFSET_THRESHOLD_MS
        ) {
          this.#bufferSizer.reset();
          this.#playbackOffsetMs = timestampOffset;

          lc.debug?.('new playback offset', timestampOffset);
        }
        bufferNeededMs =
          now -
          (timestamp + this.#playbackOffsetMs) +
          FRAME_INTERVAL_TOLERANCE_MS;

        // only consider the first poke in the message with a timestamp for
        // timestamp offsets
        break;
      }
    }
    for (const poke of pokeBody.pokes) {
      if (poke.lastMutationIDChanges[thisClientID] !== undefined) {
        lastMutationIDChangeForSelf = poke.lastMutationIDChanges[thisClientID];
      }
      // normalize timestamps by playback offset
      let normalizedTimestamp = undefined;
      if (poke.timestamp !== undefined) {
        assert(this.#playbackOffsetMs !== undefined);
        normalizedTimestamp = poke.timestamp + this.#playbackOffsetMs;
      }
      const pendingPoke: PendingPoke = {
        ...poke,
        serverBufferMs: pokeBody.debugServerBufferMs,
        playbackOffsetMs: this.#playbackOffsetMs,
        receivedTimestamp: now,
        normalizedTimestamp,
        bufferNeededMs,
      };
      this.#pokeBuffer.push(pendingPoke);
    }
    if (this.#pokeBuffer.length > 0 && !this.#pokePlaybackLoopRunning) {
      this.#startPlaybackLoop(lc);
    }
    return lastMutationIDChangeForSelf;
  }

  #startPlaybackLoop(lc: LogContext) {
    lc.debug?.('starting playback loop');
    this.#pokePlaybackLoopRunning = true;
    requestAnimationFrame(this.#rafCallback);
  }

  #rafCallback = async () => {
    const rafLC = this.#lc.withContext('rafAt', Math.floor(performance.now()));
    if (this.#pokeBuffer.length === 0) {
      rafLC.debug?.('stopping playback loop');
      this.#pokePlaybackLoopRunning = false;
      return;
    }
    requestAnimationFrame(this.#rafCallback);
    const start = performance.now();
    rafLC.debug?.(
      'raf fired, processing pokes.  Since last raf',
      start - this.#lastRafPerfTimestamp,
    );
    this.#lastRafPerfTimestamp = start;
    await this.#processPokesForFrame(rafLC);
    rafLC.debug?.('processing pokes took', performance.now() - start);
  };

  async #processPokesForFrame(lc: LogContext) {
    await this.#pokeLock.withLock(async () => {
      const now = Date.now();
      lc.debug?.('got poke lock at', now);
      const toMerge: Poke[] = [];
      const thisClientID = this.#clientID;
      let maxBufferNeededMs = Number.MIN_SAFE_INTEGER;
      let timedPokeCount = 0;
      let missedTimedPokeCount = 0;
      while (this.#pokeBuffer.length) {
        const headPoke = this.#pokeBuffer[0];
        const {normalizedTimestamp, lastMutationIDChanges} = headPoke;
        const lastMutationIDChangesClientIDs = Object.keys(
          lastMutationIDChanges,
        );
        const isThisClientsMutation =
          lastMutationIDChangesClientIDs.length === 1 &&
          lastMutationIDChangesClientIDs[0] === thisClientID;
        if (!isThisClientsMutation && normalizedTimestamp !== undefined) {
          const pokePlaybackTarget =
            normalizedTimestamp + this.#bufferSizer.bufferSizeMs;
          const pokePlaybackOffset = now - pokePlaybackTarget;
          if (pokePlaybackOffset < 0) {
            break;
          }
          const {bufferNeededMs} = headPoke;
          if (bufferNeededMs !== undefined) {
            maxBufferNeededMs = Math.max(maxBufferNeededMs, bufferNeededMs);
          }
          if (headPoke.debugOriginTimestamp !== undefined) {
            const serverReceivedLatency =
              (headPoke.debugServerReceivedTimestamp ?? 0) -
              headPoke.debugOriginTimestamp;
            const serverSentLatency =
              (headPoke.debugServerSentTimestamp ?? 0) -
              headPoke.debugOriginTimestamp;
            const clientReceivedLatency =
              headPoke.receivedTimestamp - headPoke.debugOriginTimestamp;
            const playbackLatency = now - headPoke.debugOriginTimestamp;
            this.#timedPokeLatencyTotal += playbackLatency;
            this.#recentPokeLatencies.unshift(playbackLatency);
            if (
              this.#recentPokeLatencies.length >
              this.#maxRecentPokeLatenciesSize
            ) {
              this.#recentPokeLatencies.length =
                this.#maxRecentPokeLatenciesSize;
            }
            lc.debug?.(
              'poke latency breakdown:',
              '\nserver received:',
              serverReceivedLatency,
              '(+',
              serverReceivedLatency,
              ')',
              '\nserver sent (server buffer',
              headPoke.serverBufferMs,
              '):',
              serverSentLatency,
              '(+',
              serverSentLatency - serverReceivedLatency,
              ')',
              '\nclient received:',
              clientReceivedLatency,
              '(+',
              clientReceivedLatency - serverSentLatency,
              ')',
              '\nplayback (offset',
              headPoke.playbackOffsetMs,
              ', buffer',
              this.#bufferSizer.bufferSizeMs,
              '):',
              playbackLatency,
              '(+',
              playbackLatency - clientReceivedLatency,
              ')',
            );
          }
          timedPokeCount++;
          this.#timedPokeCount++;
          if (pokePlaybackOffset > FRAME_INTERVAL_TOLERANCE_MS) {
            lc.debug?.(
              'poke',
              this.#timedPokeCount,
              'playback missed by',
              pokePlaybackOffset - FRAME_INTERVAL_TOLERANCE_MS,
            );
            this.#missedTimedPokeCount++;
            missedTimedPokeCount++;
          }
        }
        const poke = this.#pokeBuffer.shift();
        assert(poke);
        toMerge.push(poke);
      }
      if (timedPokeCount > 0) {
        this.#timedFrameCount++;
        assert(maxBufferNeededMs !== Number.MIN_SAFE_INTEGER);
        this.#bufferSizer.recordMissable(
          now,
          missedTimedPokeCount > 0,
          maxBufferNeededMs,
          lc,
        );
        if (missedTimedPokeCount > 0) {
          this.#missedTimedFrameCount++;
          lc.debug?.(
            'frame',
            this.#timedFrameCount,
            'contains',
            missedTimedPokeCount,
            'missed pokes',
          );
        }
      }
      lc.debug?.(
        'merging',
        toMerge.length,
        'remaining buffer length',
        this.#pokeBuffer.length,
      );
      const merged = mergePokes(toMerge);
      if (merged === undefined) {
        lc.debug?.('frame is empty');
        return;
      }
      try {
        const start = performance.now();
        const {lastMutationIDChanges, baseCookie, patch, cookie} = merged;
        const poke: ReplicachePoke = {
          baseCookie,
          pullResponse: {
            lastMutationIDChanges,
            patch,
            cookie,
          },
        };
        lc.debug?.('poking replicache');
        await this.#replicachePoke(poke);
        await this.#presenceManager.updatePresence(merged.presence ?? []);
        lc.debug?.('poking replicache took', performance.now() - start);
      } catch (e) {
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          await this.#onOutOfOrderPoke();
        }
      }
      lc.debug?.(
        'playback stats (misses / total = percent missed):',
        '\npokes:',
        this.#missedTimedPokeCount,
        '/',
        this.#timedPokeCount,
        '=',
        this.#missedTimedPokeCount / this.#timedPokeCount,
        '\nframes:',
        this.#missedTimedFrameCount,
        '/',
        this.#timedFrameCount,
        '=',
        this.#missedTimedFrameCount / this.#timedFrameCount,
        '\navg poke latency:',
        this.#timedPokeLatencyTotal / this.#timedPokeCount,
        '\nrecent poke latencies:',
        this.#recentPokeLatencies,
      );
    });
  }

  handleDisconnect(): void {
    this.#lc.debug?.('clearing buffer and playback offset due to disconnect');
    this.#pokeBuffer.length = 0;
    this.#playbackOffsetMs = undefined;
    this.#bufferSizer.reset();
  }
}
