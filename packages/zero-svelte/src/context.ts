import {getContext, setContext} from 'svelte';
import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  CustomMutatorDefs,
  DefaultContext,
  DefaultSchema,
  ZeroOptions,
} from './zero-client.ts';
import {Z} from './zero.svelte.ts';

const ZERO_CONTEXT = Symbol('zero');

export function setZero<
  TSchema extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  TContext extends BaseDefaultContext = DefaultContext,
>(z: Z<TSchema, MD, TContext>): Z<TSchema, MD, TContext> {
  setContext(ZERO_CONTEXT, z);
  return z;
}

export function useZero<
  TSchema extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  TContext extends BaseDefaultContext = DefaultContext,
>(): Z<TSchema, MD, TContext> {
  const z = getContext<Z<TSchema, MD, TContext> | undefined>(ZERO_CONTEXT);
  if (!z) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return z;
}

export function createUseZero<
  TSchema extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  TContext extends BaseDefaultContext = DefaultContext,
>() {
  return () => useZero<TSchema, MD, TContext>();
}

export function createZero<
  TSchema extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  TContext extends BaseDefaultContext = DefaultContext,
>(options: ZeroOptions<TSchema, MD, TContext>): Z<TSchema, MD, TContext> {
  return new Z(options);
}
