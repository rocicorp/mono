import 'shared/src/dotenv.ts';

import {runAnalyzeCLI} from '@rocicorp/zero/analyze';
import {schema} from '../shared/schema.ts';

await runAnalyzeCLI({schema});
