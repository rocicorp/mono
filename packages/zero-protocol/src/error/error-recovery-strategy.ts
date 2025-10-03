import type {Enum} from '../../../shared/src/enum.ts';
import * as ErrorRecoveryStrategyEnum from './error-recovery-strategy-enum.ts';

export {ErrorRecoveryStrategyEnum as ErrorRecoveryStrategy};
export type ErrorRecoveryStrategy = Enum<typeof ErrorRecoveryStrategyEnum>;
