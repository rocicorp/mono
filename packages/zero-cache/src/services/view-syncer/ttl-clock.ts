/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
