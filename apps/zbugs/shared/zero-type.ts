import type {Zero} from '@rocicorp/zero';
import type {AuthData} from './auth.ts';
import type {Mutators} from './mutators.ts';
import type {Queries} from './queries.ts';
import type {Schema} from './schema.ts';

export type ZeroBugs = Zero<Schema, Mutators, AuthData | undefined, Queries>;
