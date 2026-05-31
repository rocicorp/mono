import type {Enum} from '../../shared/src/enum.ts';
import * as ErrorKindEnum from './error-kind-enum.ts';

export {ErrorKindEnum as ErrorKind};
export type ErrorKind = Enum<typeof ErrorKindEnum>;
