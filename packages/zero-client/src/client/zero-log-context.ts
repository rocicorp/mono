import {LogContext} from '@rocicorp/logger';
import type {OnLogParameters} from './options.ts';

export const ZeroLogContext = LogContext<OnLogParameters>;
export type ZeroLogContext = LogContext<OnLogParameters>;
