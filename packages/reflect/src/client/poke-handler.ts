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
  minBuferSizeMs: 25,
  adjustBufferSizeIntervalMs: 10 * 1000,
} as const;
export const RESET_PLAYBACK_OFFSET_THRESHOLD_MS =
  BUFFER_SIZER_OPTIONS.maxBufferSizeMs;

export class PokeHandler {
  private readonly _replicachePoke: (poke: ReplicachePoke) => Promise<void>;
  private readonly _onOutOfOrderPoke: () => MaybePromise<void>;
  private readonly _clientIDPromise: Promise<ClientID>;
  private readonly _lcPromise: Promise<LogContext>;
  private readonly _pokeBuffer: Poke[] = [];
  private readonly _bufferSizer: BufferSizer;
  private _pokePlaybackLoopRunning = false;
  private _playbackOffset: number | undefined = undefined;
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
    const now = performance.now();
    const thisClientID = await this._clientIDPromise;
    let lastMutationIDChangeForSelf: number | undefined;
    for (const poke of pokeBody.pokes) {
      const {timestamp} = poke;
      if (timestamp !== undefined) {
        const timestampOffset = now - timestamp;
        if (
          this._playbackOffset === undefined ||
          Math.abs(timestampOffset - this._playbackOffset) >
            RESET_PLAYBACK_OFFSET_THRESHOLD_MS
        ) {
          this._bufferSizer.reset();
          this._playbackOffset = timestampOffset;

          lc.debug?.('new playback offset', timestampOffset);
        }
        this._bufferSizer.recordOffset(thisClientID, timestampOffset);
        // only consider the first poke in the message with a timestamp for
        // timestamp offsets
        break;
      }
    }
    // adjust timestamps by playback offset
    for (const poke of pokeBody.pokes) {
      if (poke.timestamp !== undefined) {
        assert(this._playbackOffset !== undefined);
        poke.timestamp = poke.timestamp + this._playbackOffset;
      }
    }
    for (const poke of pokeBody.pokes) {
      if (poke.lastMutationIDChanges[thisClientID] !== undefined) {
        lastMutationIDChangeForSelf = poke.lastMutationIDChanges[thisClientID];
      }
    }
    this._pokeBuffer.push(...pokeBody.pokes);
    if (this._pokeBuffer.length > 0 && !this._pokePlaybackLoopRunning) {
      this._startPlaybackLoop(lc);
    }
    return lastMutationIDChangeForSelf;
  }

  private _startPlaybackLoop(lc: LogContext) {
    lc.debug?.('starting playback loop');
    this._pokePlaybackLoopRunning = true;
    const rafCallback = async () => {
      const now = performance.now();
      const rafLC = (await this._lcPromise).addContext('rafAt', now);
      if (this._pokeBuffer.length === 0) {
        rafLC.debug?.('stopping playback loop');
        this._pokePlaybackLoopRunning = false;
        return;
      }
      requestAnimationFrame(rafCallback);
      const start = performance.now();
      rafLC.debug?.('raf fired, processing pokes');
      await this._processPokesForFrame(rafLC);
      rafLC.debug?.('processing pokes took', performance.now() - start);
    };
    requestAnimationFrame(rafCallback);
  }

  private async _processPokesForFrame(lc: LogContext) {
    await this._pokeLock.withLock(async () => {
      const perfNow = performance.now();
      const unixNow = Date.now();
      lc.debug?.('got poke lock at', perfNow);
      this._bufferSizer.maybeAdjustBufferSize(perfNow, lc);
      const toMerge: Poke[] = [];
      const thisClientID = await this._clientIDPromise;
      let timedPokeCount = 0;
      let missedTimedPokeCount = 0;
      while (this._pokeBuffer.length) {
        const headPoke = this._pokeBuffer[0];
        const {timestamp, lastMutationIDChanges} = headPoke;
        const lastMutationIDChangesClientIDs = Object.keys(
          lastMutationIDChanges,
        );
        const isThisClientsMutation =
          lastMutationIDChangesClientIDs.length === 1 &&
          lastMutationIDChangesClientIDs[0] === thisClientID;
        if (!isThisClientsMutation && timestamp !== undefined) {
          const pokePlaybackTarget = timestamp + this._bufferSizer.bufferSizeMs;
          const pokePlaybackOffset = Math.floor(perfNow - pokePlaybackTarget);
          if (pokePlaybackOffset < 0) {
            break;
          }
          if (headPoke.debugOriginTimestamp) {
            const latencyMs = unixNow - headPoke.debugOriginTimestamp;
            this._timedPokeLatencyTotal += latencyMs;
            lc.debug?.('poke latency ms', latencyMs);
          }
          // TODO consider systems that don't run at 60fps (supposedly new
          // ipads run RAF at 120fps).
          timedPokeCount++;
          this._timedPokeCount++;
          if (pokePlaybackOffset > 16) {
            lc.debug?.(
              'poke',
              this._timedPokeCount,
              'playback missed by',
              pokePlaybackOffset - 16,
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
        this._bufferSizer.recordMissable(missedTimedPokeCount > 0);
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
    this._playbackOffset = undefined;
    this._bufferSizer.reset();
  }
}
