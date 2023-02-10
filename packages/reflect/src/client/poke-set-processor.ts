import type {PokeBody} from '../protocol/poke';

const FRAME_DURATION = 1000 / 60;
const PLAYBACK_BUFFER = 250;

export type ApplyPoke = (poke: PokeBody) => void;
type EpochFrame = {epochStart: number; poke: PokeBody};

export class PokeSetProcessor {
  private readonly _pokes: EpochFrame[] = [];
  private readonly _applyPoke: ApplyPoke;
  private _isProcessing = false;
  private _epoch = -1;
  private _epochStartTime = 0;

  constructor(applyPoke: ApplyPoke) {
    this._applyPoke = applyPoke;
  }

  add(pokeBody: PokeBody): void {
    if (pokeBody.epoch !== this._epoch) {
      this._epoch = pokeBody.epoch;
      this._epochStartTime = Date.now() + PLAYBACK_BUFFER;
    }
    this._pokes.push({epochStart: this._epochStartTime, poke: pokeBody});
    this._startLoop();
  }

  private _startLoop(): void {
    if (this._isProcessing || this._pokes.length === 0) {
      return;
    }

    this._isProcessing = true;
    void this._loop();
  }

  private _loop() {
    const now = Date.now();

    const pokes = this._pokes;
    if (pokes.length === 0) {
      this._isProcessing = false;
      return;
    }

    while (pokes.length > 0) {
      const {epochStart, poke} = pokes[0];
      const applyTime = epochStart + poke.frame * FRAME_DURATION;

      if (applyTime > now) {
        break;
      }

      this._applyPoke(poke);
      pokes.shift();
    }

    requestAnimationFrame(() => this._loop());
  }
}
