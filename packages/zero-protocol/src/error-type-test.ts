import type {ErrorKind, errorKindSchema} from './error.js';
import type * as v from 'shared/src/valita.js';

// The following ensures ErrorKind and errorKindSchema
// are kept in sync (each type satisfies the other).
(t: ErrorKind, inferredT: v.Infer<typeof errorKindSchema>) => {
  t satisfies v.Infer<typeof errorKindSchema>;
  inferredT satisfies ErrorKind;
};
