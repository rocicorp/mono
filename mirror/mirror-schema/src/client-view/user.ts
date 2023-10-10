import type * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from '../converter.js';
import {userSchema} from '../user.js';

export {userPath} from '../user.js';

// The slice of User fields read by the cli.
// Having the cli use a constrained schema makes it easier to
// refactor/rewrite other parts of the schema.
// Pick more fields as necessary.
const userViewSchema = userSchema.pick('roles');

export type UserView = v.Infer<typeof userViewSchema>;

export const userViewDataConverter = firestoreDataConverter(userViewSchema);
