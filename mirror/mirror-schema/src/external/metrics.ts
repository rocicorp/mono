import type * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from '../converter.js';
import {monthMetricsSchema, totalMetricsSchema} from '../metrics.js';
export {
  monthMetricsPath,
  splitDate,
  totalMetricsPath,
  type DayOfMonth,
  type Hour,
  type MetricsNode,
  type Month,
} from '../metrics.js';

const monthMetricsViewSchema = monthMetricsSchema.pick('total', 'day');

export type MonthMetricsView = v.Infer<typeof monthMetricsViewSchema>;

export const monthMetricsViewDataConverter = firestoreDataConverter(
  monthMetricsViewSchema,
);

const totalMetricsViewSchema = totalMetricsSchema.pick('year');

export const totalMetricsViewDataConverter = firestoreDataConverter(
  totalMetricsViewSchema,
);
