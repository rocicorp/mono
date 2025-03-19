import {createUseZero} from '@rocicorp/zero/react';
import type {Schema} from '../../shared/schema.ts';
import type {Mutators} from '../../shared/mutators.ts';
export const useZero = createUseZero<Schema, Mutators>();
