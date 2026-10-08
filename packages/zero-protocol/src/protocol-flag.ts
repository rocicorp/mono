import type {Enum} from '../../shared/src/enum.ts';
import * as ProtocolFlagEnum from './protocol-flag-enum.ts';

export {ProtocolFlagEnum as ProtocolFlag};
export type ProtocolFlag = Enum<typeof ProtocolFlagEnum>;

/** A set of protocol flags. */
export type ProtocolFlags = ReadonlySet<ProtocolFlag>;

/** `Set`, typed so that `new ProtocolFlags()` needs no type argument. */
export const ProtocolFlags = Set<ProtocolFlag>;
