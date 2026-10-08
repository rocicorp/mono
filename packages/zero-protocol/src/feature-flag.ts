import type {Enum} from '../../shared/src/enum.ts';
import * as FeatureFlagEnum from './feature-flag-enum.ts';

export {FeatureFlagEnum as FeatureFlag};
export type FeatureFlag = Enum<typeof FeatureFlagEnum>;
