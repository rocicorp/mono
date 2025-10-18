import type {Enum} from '../../../shared/src/enum.ts';
import * as PingResultEnum from './ping-result-enum.ts';

export {PingResultEnum as PingResult};
export type PingResult = Enum<typeof PingResultEnum>;
