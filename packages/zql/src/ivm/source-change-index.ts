import type {Enum} from '../../../shared/src/enum.ts';
import * as SourceChangeIndexEnum from './source-change-index-enum.ts';

export {SourceChangeIndexEnum as SourceChangeIndex};
export type SourceChangeIndex = Enum<typeof SourceChangeIndexEnum>;
