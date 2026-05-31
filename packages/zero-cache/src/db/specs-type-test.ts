import type {Enum} from '../../../shared/src/enum.ts';
import type * as v from '../../../shared/src/valita.ts';
import type * as PostgresTypeClass from './postgres-type-class-enum.ts';
import type {pgTypeClassSchema} from './specs.ts';

type PostgresTypeClass = Enum<typeof PostgresTypeClass>;

// The following ensures TypeClass and typeClassSchema
// are kept in sync (each type satisfies the other).
(t: PostgresTypeClass, inferredT: v.Infer<typeof pgTypeClassSchema>) => {
  t satisfies v.Infer<typeof pgTypeClassSchema>;
  inferredT satisfies PostgresTypeClass;
};
