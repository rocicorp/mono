import {mergePokeBodies} from './merge-pokes.js';
import type {PokeBody} from '../protocol/poke';
import {assertNumber} from '../util/asserts.js';

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

  add(poke: PokeBody): void {
    this._pokes.push(poke);
    this._setEpochOffset(poke, performance.now());

    if (!this._isProcessing) {
      this._isProcessing = true;
      requestAnimationFrame(async () => await this._loop());
    }
  }

  private async _loop() {
    const pokes = this._pokes;

    if (pokes.length === 0) {
      this._isProcessing = false;
      return;
    }

    const now = performance.now();

    // find the last index of pokes that are ready to be applied
    let lastIndex = -1;
    for (let i = pokes.length - 1; i >= 0; i--) {
      const {epoch, frame} = pokes[i];

      const epochOffset = this._epochOffsets.get(epoch);
      assertNumber(epochOffset);

      const applyTime = frame + epochOffset + PLAYBACK_BUFFER_MS;
      if (applyTime <= now) {
        lastIndex = i;
        break;
      }
    }

    if (lastIndex !== -1) {
      const pokesToApply = this._pokes.slice(0, lastIndex + 1);

      if (pokesToApply.length > 2) {
        console.log(
          `applying pokes ${pokesToApply.length} / ${this._pokes.length}`,
        );
      }
      this._pokes.splice(0, lastIndex + 1);
      const merged = mergePokeBodies(pokesToApply);
      this._applyPoke(merged);
    }

    requestAnimationFrame(async () => await this._loop());
  }

  private _setEpochOffset(poke: PokeBody, now: number) {
    const pokeOffset = now - poke.frame;
    let epochOffset = this._epochOffsets.get(poke.epoch);

    if (
      epochOffset === undefined ||
      // TODO: threshold here is chosen randomly; find a better value
      Math.abs(pokeOffset - epochOffset) > PLAYBACK_BUFFER_MS
    ) {
      // TODO: this is probably wrong, for instance if there is a poke at the very end of a turn, and
      // nothing else in between, then we are probably playing it back too soon, and pokes need to be
      // interleaved and played back in order.
      // initialize epochOffset so that it simply plays back now
      this._epochOffsets.set(poke.epoch, pokeOffset);
      epochOffset = pokeOffset;
    }
  }
}
