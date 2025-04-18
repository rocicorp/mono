/* eslint-disable @typescript-eslint/naming-convention */

export * from '../../../zero-protocol/src/error-kind-enum.ts';
export {
  NewClientGroup,
  SchemaVersionNotSupported,
  VersionNotSupported,
} from './update-needed-reason-type-enum.ts';

export const Poke = 'Poke';
export const Mutation = 'Mutation';
export const Push = 'Push';
export const Metrics = 'Metrics';
export const Unknown = 'Unknown';
export const InvalidState = 'InvalidState';
export const Network = 'Network';

export type Poke = typeof Poke;
export type Mutation = typeof Mutation;
export type Push = typeof Push;
export type Metrics = typeof Metrics;
export type Unknown = typeof Unknown;
export type InvalidState = typeof InvalidState;
export type Network = typeof Network;
