import {mergePokeBodies} from './merge-pokes.js';
import type {PokeBody} from '../protocol/poke';

const PLAYBACK_BUFFER_MS = 250;

export type ApplyPoke = (poke: PokeBody) => void;

export class PokeSetProcessor {
  private readonly _pokes: PokeBody[] = [];
  private readonly _applyPoke: ApplyPoke;
  private _isProcessing = false;
  private _epochOffsets = new Map<number, number>();

  constructor(applyPoke: ApplyPoke) {
    this._applyPoke = applyPoke;
  }

  add(pokeBody: PokeBody): void {
    this._pokes.push(pokeBody);
    void this._startLoop();
  }

  private _startLoop() {
    if (this._isProcessing) {
      return;
    }

    this._isProcessing = true;
    void this._loop();
  }

  private async _loop() {
    const pokes = this._pokes;
    if (pokes.length === 0) {
      this._isProcessing = false;
      return;
    }

    const pokesToApply: PokeBody[] = [];
    while (pokes.length > 0) {
      const now = performance.now();
      const poke = pokes[0];

      let applyTime: number;
      // special handling of FF pokes
      if (poke.frame === 0) {
        applyTime = now;
      } else {
        let epochOffset = this._epochOffsets.get(poke.epoch);
        if (epochOffset === undefined) {
          // TODO: this is probably wrong, for instance if there is a poke at the very end of a turn, and
          // nothing else in between, then we are probably playing it back too soon, and pokes need to be
          // interleaved and played back in order.
          // initialize epochOffset so that it simply plays back now
          epochOffset = now - poke.frame;
          this._epochOffsets.set(poke.epoch, epochOffset);
        }

        applyTime = poke.frame + epochOffset + PLAYBACK_BUFFER_MS;
      }

      if (applyTime > now) {
        break;
      }
      pokesToApply.push(poke);
      pokes.shift();
    }

    if (pokesToApply.length > 0) {
      const merged = mergePokeBodies(pokesToApply);
      await this._applyPoke(merged);
    }

    requestAnimationFrame(async () => await this._loop());
  }
}
