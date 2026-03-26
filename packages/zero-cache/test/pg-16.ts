import {runPostgresContainer} from './pg-container-setup.ts';

export default runPostgresContainer('postgres:16-alpine', 'UTC');
