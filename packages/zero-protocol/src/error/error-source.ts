import type {Enum} from '../../../shared/src/enum.ts';
import * as ErrorSourceEnum from './error-source-enum.ts';

export {ErrorSourceEnum as ErrorSource};
export type ErrorSource = Enum<typeof ErrorSourceEnum>;
