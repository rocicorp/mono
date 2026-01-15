import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:15.8-alpine3.20', 'UTC');
