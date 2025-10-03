import type {Enum} from '../../../shared/src/enum.ts';
import * as ErrorSeverityEnum from './error-severity-enum.ts';

export {ErrorSeverityEnum as ErrorSeverity};
export type ErrorSeverity = Enum<typeof ErrorSeverityEnum>;
