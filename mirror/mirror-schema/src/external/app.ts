import type * as v from 'shared/src/valita.js';
import {appSchema} from '../app.js';
import {firestoreDataConverter} from '../converter.js';
import {deploymentViewSchema} from './deployment.js';

// The slice of App fields read by the cli.
// Having the cli use a constrained schema makes it easier to
// refactor/rewrite other parts of the schema.
// Pick more fields as necessary.
const appViewSchema = appSchema
  .pick('name', 'runningDeployment', 'teamID')
  .extend({
    runningDeployment: deploymentViewSchema.optional(),
  });

export type AppView = v.Infer<typeof appViewSchema>;

export const appViewDataConverter = firestoreDataConverter(appViewSchema);

// APP_COLLECTION and appPath() are defined in deployment.js to avoid a cyclic
// dependency (which otherwise breaks mjs targets). Re-export them here to be
// consistent with other schema files.
export {APP_COLLECTION, appPath} from '../deployment.js';

export {isValidSubdomain as isValidAppName} from '../team.js';
