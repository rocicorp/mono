import type {Enum} from '../../shared/src/enum.ts';
import * as ProtocolFlagEnum from './protocol-flag-enum.ts';

export {ProtocolFlagEnum as ProtocolFlag};
export type ProtocolFlag = Enum<typeof ProtocolFlagEnum>;

// Spelled out instead of using ProtocolFlag: TypeScript 6 can't emit
// declarations that refer to a name this file both re-exports and declares.

/** A set of protocol flags. */
export type ProtocolFlags = ReadonlySet<Enum<typeof ProtocolFlagEnum>>;

/** `Set`, typed so that `new ProtocolFlags()` needs no type argument. */
export const ProtocolFlags = Set<Enum<typeof ProtocolFlagEnum>>;
