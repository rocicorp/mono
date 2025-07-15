import * as v from '../../../../shared/src/valita.ts';

declare const ttlClockTag: unique symbol;

export type TTLClock = {[ttlClockTag]: true};

export const ttlClockSchema = v.number() as v.Type<unknown> as v.Type<TTLClock>;

export function ttlClockAsNumber(ttlClock: TTLClock): number {
  return ttlClock as unknown as number;
}

export function ttlClockFromNumber(ttlClock: number): TTLClock {
  return ttlClock as unknown as TTLClock;
}
