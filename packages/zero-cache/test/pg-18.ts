import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:18-alpine', 'UTC+1');
