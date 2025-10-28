import type {Enum} from '../../shared/src/enum.ts';
import * as ErrorOriginEnum from './error-origin-enum.ts';

export {ErrorOriginEnum as ErrorOrigin};
export type ErrorOrigin = Enum<typeof ErrorOriginEnum>;
