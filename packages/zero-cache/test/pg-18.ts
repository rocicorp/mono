import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:18.1-alpine3.23', 'UTC+1');
