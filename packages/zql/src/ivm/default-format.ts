import type {Format} from './view.ts';

export const defaultFormat: Format = {
  singular: false,
  relationships: {},
} as const;
