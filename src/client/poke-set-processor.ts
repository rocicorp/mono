import type {PokeBody} from '../protocol/poke';
import type {PokeSet} from '../protocol/poke-set';
import {assert} from '../util/asserts';

const DEFAULT_PROCESS_RATE_MS = 20;

export type ApplyPoke = (poke: PokeBody) => void;

export class PokeSetProcessor {
  private readonly _pokeSets: PokeSet[] = [];
  private readonly _applyPoke: ApplyPoke;
  private _isProcessing = false;

  private _curPokeSet: PokeSet | undefined = undefined;
  private _startFrame = 0;
  private _startTime = 0;

  constructor(applyPoke: ApplyPoke) {
    this._applyPoke = applyPoke;
  }

  add(pokeSet: PokeSet): void {
    this._pokeSets.push(pokeSet);
    this.applyPokeSets();
  }

  private applyPokeSets(): void {
    if (this._isProcessing || this._pokeSets.length === 0) {
      return;
    }

    this._isProcessing = true;
    this._curPokeSet = this._pokeSets.shift();
    assert(this._curPokeSet);

    this._startFrame = this._curPokeSet.frames[0].frame;
    this._startTime = performance.now();
    void this._applyCurPokeSet();
  }

  private _applyCurPokeSet() {
    const dt = performance.now() - this._startTime;

    if (!this._curPokeSet) {
      this._isProcessing = false;
      return;
    }

    const pokeFrames = this._curPokeSet.frames;
    const rate = this._curPokeSet.rate || DEFAULT_PROCESS_RATE_MS;

    while (pokeFrames.length > 0) {
      const {frame, poke} = pokeFrames[0];
      const applyTime = (frame - this._startFrame) * rate;

      if (applyTime >= dt) {
        break;
      }

      this._applyPoke(poke);
      pokeFrames.shift();
    }

    if (pokeFrames.length === 0) {
      this._curPokeSet = this._pokeSets.shift();
    }

    requestAnimationFrame(() => this._applyCurPokeSet());
  }
}
