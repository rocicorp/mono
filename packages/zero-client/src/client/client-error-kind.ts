import type {Enum} from '../../../shared/src/enum.ts';
import * as ClientErrorKindEnum from './client-error-kind-enum.ts';

export {ClientErrorKindEnum as ClientErrorKind};
export type ClientErrorKind = Enum<typeof ClientErrorKindEnum>;
