import '../../../packages/shared/src/dotenv.ts';

import {runAnalyzeCli} from '../../../packages/zero/src/analyze.ts';
import {schema} from '../shared/schema.ts';

await runAnalyzeCli({schema});
