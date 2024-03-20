import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {Poke, PokeBody} from 'reflect-protocol';
import type {ClientID, MaybePromise, Poke as ReplicachePoke} from 'replicache';
import {mergePokes} from './merge-pokes.js';

export class PokeHandler {
  readonly #replicachePoke: (poke: ReplicachePoke) => Promise<void>;
  readonly #onOutOfOrderPoke: () => MaybePromise<void>;
  readonly #clientID: ClientID;
  readonly #lc: LogContext;
  readonly #pokeBuffer: Poke[] = [];
  #pokePlaybackLoopRunning = false;
  #lastRafPerfTimestamp = 0;
  // Serializes calls to this._replicachePoke otherwise we can cause out of
  // order poke errors.
  readonly #pokeLock = new Lock();

  constructor(
    replicachePoke: (poke: ReplicachePoke) => Promise<void>,
    onOutOfOrderPoke: () => MaybePromise<void>,
    clientID: ClientID,
    lc: LogContext,
  ) {
    this.#replicachePoke = replicachePoke;
    this.#onOutOfOrderPoke = onOutOfOrderPoke;
    this.#clientID = clientID;
    this.#lc = lc.withContext('PokeHandler');
  }

  handlePoke(pokeBody: PokeBody): number | undefined {
    const lc = this.#lc.withContext('requestID', pokeBody.requestID);
    lc.debug?.('Applying poke', pokeBody);
    if (pokeBody.debugServerBufferMs) {
      lc.debug?.('server buffer ms', pokeBody.debugServerBufferMs);
    }
    const thisClientID = this.#clientID;
    let lastMutationIDChangeForSelf: number | undefined;
    for (const poke of pokeBody.pokes) {
      if (poke.lastMutationIDChanges[thisClientID] !== undefined) {
        lastMutationIDChangeForSelf = poke.lastMutationIDChanges[thisClientID];
      }
      // normalize timestamps by playback offset
      this.#pokeBuffer.push(poke);
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
      lc.debug?.('merging', this.#pokeBuffer.length);
      const merged = mergePokes(this.#pokeBuffer);
      this.#pokeBuffer.length = 0;
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
        lc.debug?.('poking replicache took', performance.now() - start);
      } catch (e) {
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          await this.#onOutOfOrderPoke();
        }
      }
    });
  }

  handleDisconnect(): void {
    this.#lc.debug?.('clearing buffer due to disconnect');
    this.#pokeBuffer.length = 0;
  }
}
