import type {Enum} from '../../../shared/src/enum.ts';
import * as MetricNameEnum from './metric-name-enum.ts';

export {MetricNameEnum as MetricName};
export type MetricName = Enum<typeof MetricNameEnum>;
