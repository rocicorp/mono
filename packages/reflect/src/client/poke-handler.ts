import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {Poke, PokeBody} from 'reflect-protocol';
import type {ClientID, MaybePromise, Poke as ReplicachePoke} from 'replicache';
import {assert} from 'shared/asserts.js';
import {mergePokes} from './merge-pokes.js';
import {BufferSizer} from 'shared/buffer-sizer.js';

export const BUFFER_SIZER_OPTIONS = {
  initialBufferSizeMs: 250,
  maxBufferSizeMs: 1000,
  minBuferSizeMs: -1000,
  adjustBufferSizeIntervalMs: 10 * 1000,
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

type PendingPoke = Poke & {
  normalizedTimestamp?: number | undefined;
  playbackOffsetMs?: number | undefined;
  bufferNeededMs?: number | undefined;
  serverBufferMs?: number | undefined;
  receivedTimestamp: number;
};

export class PokeHandler {
  private readonly _replicachePoke: (poke: ReplicachePoke) => Promise<void>;
  private readonly _onOutOfOrderPoke: () => MaybePromise<void>;
  private readonly _clientIDPromise: Promise<ClientID>;
  private readonly _lcPromise: Promise<LogContext>;
  private readonly _pokeBuffer: PendingPoke[] = [];
  private readonly _bufferSizer: BufferSizer;
  private _pokePlaybackLoopRunning = false;
  private _lastRafPerfTimestamp = 0;
  private _playbackOffsetMs: number | undefined = undefined;
  // Serializes calls to this._replicachePoke otherwise we can cause out of
  // order poke errors.
  private readonly _pokeLock = new Lock();
  private _timedPokeCount = 0;
  private _missedTimedPokeCount = 0;
  private _timedFrameCount = 0;
  private _missedTimedFrameCount = 0;
  private _timedPokeLatencyTotal = 0;

  constructor(
    replicachePoke: (poke: ReplicachePoke) => Promise<void>,
    onOutOfOrderPoke: () => MaybePromise<void>,
    clientIDPromise: Promise<ClientID>,
    lcPromise: Promise<LogContext>,
    bufferSizer = new BufferSizer(BUFFER_SIZER_OPTIONS),
  ) {
    this._replicachePoke = replicachePoke;
    this._onOutOfOrderPoke = onOutOfOrderPoke;
    this._clientIDPromise = clientIDPromise;
    this._lcPromise = lcPromise.then(lc => lc.addContext('PokeHandler'));
    this._bufferSizer = bufferSizer;
  }

  async handlePoke(pokeBody: PokeBody): Promise<number | undefined> {
    const lc = (await this._lcPromise).addContext(
      'requestID',
      pokeBody.requestID,
    );
    lc.debug?.('Applying poke', pokeBody);
    if (pokeBody.debugServerBufferMs) {
      lc.debug?.('server buffer ms', pokeBody.debugServerBufferMs);
    }
    const now = Date.now();
    const thisClientID = await this._clientIDPromise;
    let lastMutationIDChangeForSelf: number | undefined;
    let bufferNeededMs = undefined;
    for (const poke of pokeBody.pokes) {
      const {timestamp} = poke;
      if (timestamp !== undefined) {
        const timestampOffset = now - timestamp;
        if (
          this._playbackOffsetMs === undefined ||
          Math.abs(timestampOffset - this._playbackOffsetMs) >
            RESET_PLAYBACK_OFFSET_THRESHOLD_MS
        ) {
          this._bufferSizer.reset();
          this._playbackOffsetMs = timestampOffset;

          lc.debug?.('new playback offset', timestampOffset);
        }
        bufferNeededMs =
          now -
          (timestamp + this._playbackOffsetMs) +
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
        assert(this._playbackOffsetMs !== undefined);
        normalizedTimestamp = poke.timestamp + this._playbackOffsetMs;
      }
      const pendingPoke: PendingPoke = {
        ...poke,
        serverBufferMs: pokeBody.debugServerBufferMs,
        playbackOffsetMs: this._playbackOffsetMs,
        receivedTimestamp: now,
        normalizedTimestamp,
        bufferNeededMs,
      };
      this._pokeBuffer.push(pendingPoke);
    }
    if (this._pokeBuffer.length > 0 && !this._pokePlaybackLoopRunning) {
      this._startPlaybackLoop(lc);
    }
    return lastMutationIDChangeForSelf;
  }

  private _startPlaybackLoop(lc: LogContext) {
    lc.debug?.('starting playback loop');
    this._pokePlaybackLoopRunning = true;
    const rafCallback = async () => {
      const rafLC = (await this._lcPromise).addContext(
        'rafAt',
        Math.floor(performance.now()),
      );
      if (this._pokeBuffer.length === 0) {
        rafLC.debug?.('stopping playback loop');
        this._pokePlaybackLoopRunning = false;
        return;
      }
      requestAnimationFrame(rafCallback);
      const start = performance.now();
      rafLC.debug?.(
        'raf fired, processing pokes.  Since last raf',
        start - this._lastRafPerfTimestamp,
      );
      this._lastRafPerfTimestamp = start;
      await this._processPokesForFrame(rafLC);
      rafLC.debug?.('processing pokes took', performance.now() - start);
    };
    requestAnimationFrame(rafCallback);
  }

  private async _processPokesForFrame(lc: LogContext) {
    await this._pokeLock.withLock(async () => {
      const now = Date.now();
      lc.debug?.('got poke lock at', now);
      const toMerge: Poke[] = [];
      const thisClientID = await this._clientIDPromise;
      let maxBufferNeededMs = Number.MIN_SAFE_INTEGER;
      let timedPokeCount = 0;
      let missedTimedPokeCount = 0;
      while (this._pokeBuffer.length) {
        const headPoke = this._pokeBuffer[0];
        const {normalizedTimestamp, lastMutationIDChanges} = headPoke;
        const lastMutationIDChangesClientIDs = Object.keys(
          lastMutationIDChanges,
        );
        const isThisClientsMutation =
          lastMutationIDChangesClientIDs.length === 1 &&
          lastMutationIDChangesClientIDs[0] === thisClientID;
        if (!isThisClientsMutation && normalizedTimestamp !== undefined) {
          const pokePlaybackTarget =
            normalizedTimestamp + this._bufferSizer.bufferSizeMs;
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
            this._timedPokeLatencyTotal += playbackLatency;
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
              this._bufferSizer.bufferSizeMs,
              '):',
              playbackLatency,
              '(+',
              playbackLatency - clientReceivedLatency,
              ')',
            );
          }
          timedPokeCount++;
          this._timedPokeCount++;
          if (pokePlaybackOffset > FRAME_INTERVAL_TOLERANCE_MS) {
            lc.debug?.(
              'poke',
              this._timedPokeCount,
              'playback missed by',
              pokePlaybackOffset - FRAME_INTERVAL_TOLERANCE_MS,
            );
            this._missedTimedPokeCount++;
            missedTimedPokeCount++;
          }
        }
        const poke = this._pokeBuffer.shift();
        assert(poke);
        toMerge.push(poke);
      }
      if (timedPokeCount > 0) {
        this._timedFrameCount++;
        assert(maxBufferNeededMs !== Number.MIN_SAFE_INTEGER);
        this._bufferSizer.recordMissable(
          now,
          missedTimedPokeCount > 0,
          maxBufferNeededMs,
          lc,
        );
        if (missedTimedPokeCount > 0) {
          this._missedTimedFrameCount++;
          lc.debug?.(
            'frame',
            this._timedFrameCount,
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
        this._pokeBuffer.length,
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
        await this._replicachePoke(poke);
        lc.debug?.('poking replicache took', performance.now() - start);
      } catch (e) {
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          await this._onOutOfOrderPoke();
        }
      }
      lc.debug?.(
        'playback stats (misses / total = percent missed):',
        '\npokes:',
        this._missedTimedPokeCount,
        '/',
        this._timedPokeCount,
        '=',
        this._missedTimedPokeCount / this._timedPokeCount,
        '\nframes:',
        this._missedTimedFrameCount,
        '/',
        this._timedFrameCount,
        '=',
        this._missedTimedFrameCount / this._timedFrameCount,
        '\navg poke latency:',
        this._timedPokeLatencyTotal / this._timedPokeCount,
      );
    });
  }

  async handleDisconnect(): Promise<void> {
    (await this._lcPromise).debug?.(
      'clearing buffer and playback offset due to disconnect',
    );
    this._pokeBuffer.length = 0;
    this._playbackOffsetMs = undefined;
    this._bufferSizer.reset();
  }
}
