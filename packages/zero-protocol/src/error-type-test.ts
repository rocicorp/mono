import type * as v from '../../shared/src/valita.js';
import type {errorBodySchema, ErrorKind} from './error.js';

// The following ensures ErrorKind and errorBodySchema['kind']
// are kept in sync (each type satisfies the other).
(t: ErrorKind, inferredT: v.Infer<typeof errorBodySchema>) => {
  t satisfies v.Infer<typeof errorBodySchema>['kind'];
  inferredT['kind'] satisfies ErrorKind;
};
