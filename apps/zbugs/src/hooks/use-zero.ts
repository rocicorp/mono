import {createUseZero} from '@rocicorp/zero/react';
import type {AuthData} from '../../shared/auth.ts';
import type {Mutators} from '../../shared/mutators.ts';
import type {Queries} from '../../shared/queries.ts';
import type {Schema} from '../../shared/schema.ts';

export const useZero = createUseZero<
  Schema,
  Mutators,
  AuthData | undefined,
  Queries
>();
