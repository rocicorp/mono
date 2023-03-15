import type {Poke, PokeBody} from 'reflect-protocol';
import {Lock} from '@rocicorp/lock';
import type {ClientID, MaybePromise, PokeDD31} from 'replicache';
import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared';
import {mergePokes} from './merge-pokes.js';

// TODO: make buffer dynamic
const PLAYBACK_BUFFER_MS = 250;
const RESET_PLAYBACK_OFFSET_THRESHOLD_MS = 1000;

export class PokeHandler {
  private readonly _replicachePoke: (poke: PokeDD31) => Promise<void>;
  private readonly _onOutOfOrderPoke: () => MaybePromise<void>;
  private readonly _clientIDPromise: Promise<ClientID>;
  private readonly _lcPromise: Promise<LogContext>;
  private readonly _pokeBuffer: Poke[] = [];
  private _pokePlaybackLoopRunning = false;
  private _playbackOffset = -1;
  // Serializes calls to this._replicachePoke otherwise we can cause out of
  // order poke errors.
  private readonly _pokeLock = new Lock();

  constructor(
    replicachePoke: (poke: PokeDD31) => Promise<void>,
    onOutOfOrderPoke: () => MaybePromise<void>,
    clientIDPromise: Promise<ClientID>,
    lcPromise: Promise<LogContext>,
  ) {
    this._replicachePoke = replicachePoke;
    this._onOutOfOrderPoke = onOutOfOrderPoke;
    this._clientIDPromise = clientIDPromise;
    this._lcPromise = lcPromise.then(lc => lc.addContext('PokeHandler'));
  }

  async handlePoke(pokeBody: PokeBody): Promise<number | undefined> {
    const lc = (await this._lcPromise).addContext(
      'requestID',
      pokeBody.requestID,
    );
    lc.debug?.('Applying poke', pokeBody);
    const now = performance.now();
    const thisClientID = await this._clientIDPromise;
    let lastMutationIDChangeForSelf: number | undefined;
    for (const poke of pokeBody.pokes) {
      const {timestamp} = poke;
      if (timestamp !== undefined) {
        const timestampOffset = now - timestamp;
        if (
          this._playbackOffset === -1 ||
          Math.abs(timestamp - this._playbackOffset) >
            RESET_PLAYBACK_OFFSET_THRESHOLD_MS
        ) {
          this._playbackOffset = timestampOffset;
          lc.debug?.('new playback offset', timestampOffset);
        }
      }
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
      lc.debug?.('got poke lock at', performance.now());
      const toMerge: Poke[] = [];
      const now = performance.now();
      const thisClientID = await this._clientIDPromise;
      let misses = 0;
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
          const playbackOffset = this._playbackOffset;
          assert(playbackOffset !== -1);
          const pokePlaybackTarget =
            timestamp + playbackOffset + PLAYBACK_BUFFER_MS;
          const pokePlaybackOffset = now - pokePlaybackTarget;
          if (pokePlaybackOffset < 0) {
            break;
          }
          // TODO consider systems that don't run at 60fps (supposedly new
          // ipads run RAF at 120fps).
          if (pokePlaybackOffset > 16) {
            lc.debug?.('poke playback missed by', pokePlaybackOffset - 16);
            misses++;
          }
        }
        const poke = this._pokeBuffer.shift();
        assert(poke);
        toMerge.push(poke);
      }
      if (misses > 0) {
        lc.debug?.('frame contains', misses, 'missed pokes');
      }
      lc.debug?.(
        'merging',
        toMerge.length,
        'remaining buffer length',
        this._pokeBuffer.length,
      );
      const merged = mergePokes(toMerge);
      if (merged === undefined) {
        return;
      }
      try {
        const start = performance.now();
        const {lastMutationIDChanges, baseCookie, patch, cookie} = merged;
        const pokeDD31: PokeDD31 = {
          baseCookie,
          pullResponse: {
            lastMutationIDChanges,
            patch,
            cookie,
          },
        };
        lc.debug?.('poking replicache');
        await this._replicachePoke(pokeDD31);
        lc.debug?.('poking replicache took', performance.now() - start);
      } catch (e) {
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          await this._onOutOfOrderPoke();
        }
      }
    });
  }

  async handleDisconnect(): Promise<void> {
    (await this._lcPromise).debug?.(
      'clearing buffer and playback offset due to disconnect',
    );
    this._pokeBuffer.length = 0;
    this._playbackOffset = -1;
  }
}
