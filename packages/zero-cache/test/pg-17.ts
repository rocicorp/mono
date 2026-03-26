import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:17-alpine', 'UTC+1');
