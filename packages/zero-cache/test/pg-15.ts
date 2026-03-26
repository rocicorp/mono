import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:15-alpine', 'UTC');
