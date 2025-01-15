import type {Enum} from '../../../shared/src/enum.js';
import type * as v from '../../../shared/src/valita.js';
import * as PostgresTypeClass from './postgres-type-class-enum.js';
import type {pgTypeClassSchema} from './specs.js';

type PostgresTypeClass = Enum<typeof PostgresTypeClass>;

// The following ensures TypeClass and typeClassSchema
// are kept in sync (each type satisfies the other).
(t: PostgresTypeClass, inferredT: v.Infer<typeof pgTypeClassSchema>) => {
  t satisfies v.Infer<typeof pgTypeClassSchema>;
  inferredT satisfies PostgresTypeClass;
};
