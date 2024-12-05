import 'dotenv/config';

import {getZeroConfig} from '../config/zero-config.js';
import {getSchema} from '../auth/load-schema.js';
import {transformAndHashQuery} from '../auth/read-authorizer.js';

const config = getZeroConfig();
const schema = await getSchema(config);

const queryAST = process.argv[2];
const query = JSON.parse(queryAST);

console.log(
  JSON.stringify(transformAndHashQuery(query, schema.permissions, {}), null, 2),
);
