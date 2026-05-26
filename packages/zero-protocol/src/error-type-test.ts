import type {Enum} from '../../shared/src/enum.ts';
import type * as v from '../../shared/src/valita.ts';
import type * as ErrorKind from './error-kind-enum.ts';
import type {errorBodySchema} from './error.ts';

type ErrorKind = Enum<typeof ErrorKind>;

// The following ensures ErrorKind and errorBodySchema['kind']
// are kept in sync (each type satisfies the other).
(t: ErrorKind, inferredT: v.Infer<typeof errorBodySchema>) => {
  t satisfies v.Infer<typeof errorBodySchema>['kind'];
  inferredT['kind'] satisfies ErrorKind;
};
