import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:17.0-alpine3.20', 'UTC+1');
