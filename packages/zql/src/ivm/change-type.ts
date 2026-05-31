import type {Enum} from '../../../shared/src/enum.ts';
import * as ChangeTypeEnum from './change-type-enum.ts';

export {ChangeTypeEnum as ChangeType};
export type ChangeType = Enum<typeof ChangeTypeEnum>;
