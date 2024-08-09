import {createTest as countIntegrationTest} from '../count-integration.js';
import {newSqliteZero} from './new-zql-lite-zero.js';
import {createTest as distinctIntegrationTest} from '../distinct-integration.js';

countIntegrationTest(newSqliteZero);
distinctIntegrationTest(newSqliteZero);
