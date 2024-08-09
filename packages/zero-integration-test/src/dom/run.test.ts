import {createTest as countIntegrationTest} from '../count-integration.js';
import {createTest as distinctIntegrationTest} from '../distinct-integration.js';
import {newZero} from './newzero.js';

countIntegrationTest(newZero);
distinctIntegrationTest(newZero);
