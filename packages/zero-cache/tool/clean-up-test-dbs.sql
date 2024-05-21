-- Run this from a psql console to clean up the test DBs created by `npm run pg-tests`.
-- When tests are successful, cleanup is automatic, but test failures can result in
-- orphaned state. Specifically, active subscriptions and slots prevent databases from
-- being able to be dropped.
--
-- \i /{PATH}/{TO}/mono/packages/zero-cache/tool/clean-up-test-dbs.sql

\c postgres
CREATE OR REPLACE FUNCTION DbExists(name TEXT) RETURNS BOOL AS $$
SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = name)
$$ LANGUAGE SQL;

SELECT DbExists('initial_sync_replica') as db_exists \gset
\if :db_exists
\c initial_sync_replica
ALTER SUBSCRIPTION test_sync DISABLE;
ALTER SUBSCRIPTION test_sync SET(slot_name=NONE);
DROP SUBSCRIPTION test_sync;
\c postgres
\endif

SELECT DbExists('initial_sync_upstream') AS db_exists \gset
\if :db_exists
\c initial_sync_upstream
SELECT * FROM pg_drop_replication_slot('zero_slot_initial_sync_test_id');
\c postgres
\endif

SELECT DbExists('sync_schema_migration_replica') AS db_exists \gset
\if :db_exists
\c sync_schema_migration_replica
ALTER SUBSCRIPTION zero_sync DISABLE;
ALTER SUBSCRIPTION zero_sync SET(slot_name=NONE);
DROP SUBSCRIPTION zero_sync;
\c postgres
\endif

SELECT DbExists('sync_schema_migration_upstream') AS db_exists \gset
\if :db_exists
\c sync_schema_migration_upstream
SELECT * FROM pg_drop_replication_slot('zero_slot_sync_schema_test_id');
\c postgres
\endif

-- Dropping the databases is optional. All tests first drop the databases they
-- use before (re-)creating them.
DROP DATABASE IF EXISTS initial_sync_replica WITH (FORCE);
DROP DATABASE IF EXISTS initial_sync_upstream WITH (FORCE);
DROP DATABASE IF EXISTS incremental_sync_test_upstream WITH (FORCE);
DROP DATABASE IF EXISTS incremental_sync_test_replica WITH (FORCE);
DROP DATABASE IF EXISTS sync_schema_migration_replica WITH (FORCE);
DROP DATABASE IF EXISTS sync_schema_migration_upstream WITH (FORCE);
DROP DATABASE IF EXISTS migration_test WITH (FORCE);
DROP DATABASE IF EXISTS create_tables_test WITH (FORCE);
DROP DATABASE IF EXISTS published_tables_test WITH (FORCE);
DROP DATABASE IF EXISTS transaction_pool_test WITH (FORCE);
DROP DATABASE IF EXISTS invalidation_test WITH (FORCE);
DROP DATABASE IF EXISTS pg_test WITH (FORCE);