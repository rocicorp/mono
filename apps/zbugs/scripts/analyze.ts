import '../../../packages/shared/src/dotenv.ts';

import {runAnalyzeCLI} from '../../../packages/zero/src/analyze.ts';
import {schema} from '../shared/schema.ts';

await runAnalyzeCLI({schema});
