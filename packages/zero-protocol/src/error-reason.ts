import type {Enum} from '../../shared/src/enum.ts';
import * as ErrorReasonEnum from './error-reason-enum.ts';

export {ErrorReasonEnum as ErrorReason};
export type ErrorReason = Enum<typeof ErrorReasonEnum>;
