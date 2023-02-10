import type {PokeBody} from '../protocol/poke';

// unused, set on the server for now
const PLAYBACK_BUFFER = 0;

export type ApplyPoke = (poke: PokeBody) => void;

export class PokeSetProcessor {
  private readonly _pokes: PokeBody[] = [];
  private readonly _applyPoke: ApplyPoke;
  private _isProcessing = false;

  constructor(applyPoke: ApplyPoke) {
    this._applyPoke = applyPoke;
  }

  add(pokeBody: PokeBody): void {
    this._pokes.push(pokeBody);
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
      const poke = pokes[0];
      const applyTime = poke.timestamp + PLAYBACK_BUFFER;

      if (applyTime > now) {
        break;
      }

      this._applyPoke(poke);
      pokes.shift();
    }

    requestAnimationFrame(() => this._loop());
  }
}
