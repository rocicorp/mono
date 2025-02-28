import {createUseZero} from '@rocicorp/zero/react';
import type {Schema} from '../../schema.ts';
import type {Mutators} from '../../mutators.ts';
export const useZero = createUseZero<Schema, Mutators>();
