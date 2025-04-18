import type {Enum} from '../../../shared/src/enum.ts';
import * as OnErrorKindEnum from './on-error-kind-enum.ts';

export {OnErrorKindEnum as OnErrorKind};
export type OnErrorKind = Enum<typeof OnErrorKindEnum>;
