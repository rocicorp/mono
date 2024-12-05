import 'dotenv/config';

import {getZeroConfig} from '../config/zero-config.js';
import {getSchema} from '../auth/load-schema.js';
import {transformAndHashQuery} from '../auth/read-authorizer.js';
import {must} from '../../../shared/src/must.js';

const config = getZeroConfig();
const schema = await getSchema(config);

const query = JSON.parse(must(config.ast));

console.log(
  JSON.stringify(transformAndHashQuery(query, schema.permissions, {}).query),
);
