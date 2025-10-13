import {LogContext} from '@rocicorp/logger';
import type {OnErrorParameters} from './on-error.ts';

export const ZeroLogContext = LogContext<OnErrorParameters>;
export type ZeroLogContext = LogContext<OnErrorParameters>;
