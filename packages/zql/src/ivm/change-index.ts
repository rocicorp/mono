import type {Enum} from '../../../shared/src/enum.ts';
import * as ChangeIndexEnum from './change-index-enum.ts';

export {ChangeIndexEnum as ChangeIndex};
export type ChangeIndex = Enum<typeof ChangeIndexEnum>;
