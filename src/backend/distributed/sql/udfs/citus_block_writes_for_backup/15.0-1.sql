CREATE OR REPLACE FUNCTION pg_catalog.citus_block_writes_for_backup(
    timeout_ms int DEFAULT 300000)
RETURNS boolean
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_block_writes_for_backup$$;
COMMENT ON FUNCTION pg_catalog.citus_block_writes_for_backup(int)
IS 'block distributed 2PC writes across the Citus cluster for backup';
