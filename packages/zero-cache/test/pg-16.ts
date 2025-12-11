import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:16.4-alpine3.20', 'UTC');
