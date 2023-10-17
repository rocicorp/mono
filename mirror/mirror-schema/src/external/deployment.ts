import type * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from '../converter.js';
import {deploymentSchema, deploymentSpecSchema} from '../deployment.js';

// The slice of Deployment fields read by the cli.
// Having the cli use a constrained schema makes it easier to
// refactor/rewrite other parts of the schema.
// Pick more fields as necessary.
export const deploymentViewSchema = deploymentSchema
  .pick('status', 'statusMessage', 'spec')
  .extend({spec: deploymentSpecSchema.pick('hostname', 'serverVersion')});

export type DeploymentView = v.Infer<typeof deploymentViewSchema>;

export const deploymentViewDataConverter =
  firestoreDataConverter(deploymentViewSchema);

export {
  APP_DEPLOYMENTS_COLLECTION_ID,
  defaultOptions,
  deploymentPath,
  deploymentsCollection,
} from '../deployment.js';
