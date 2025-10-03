import type {Enum} from '../../../shared/src/enum.ts';
import * as ErrorScopeEnum from './error-scope-enum.ts';

export {ErrorScopeEnum as ErrorScope};
export type ErrorScope = Enum<typeof ErrorScopeEnum>;
