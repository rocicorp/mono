import type * as v from '../../../shared/src/valita.js';
import type {PostgresTypeClass, pgTypeClassSchema} from './specs.js';

// The following ensures TypeClass and typeClassSchema
// are kept in sync (each type satisfies the other).
(t: PostgresTypeClass, inferredT: v.Infer<typeof pgTypeClassSchema>) => {
  t satisfies v.Infer<typeof pgTypeClassSchema>;
  inferredT satisfies PostgresTypeClass;
};
