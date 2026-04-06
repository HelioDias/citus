CREATE OR REPLACE FUNCTION pg_catalog.citus_unblock_writes_for_backup()
RETURNS boolean
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_unblock_writes_for_backup$$;
COMMENT ON FUNCTION pg_catalog.citus_unblock_writes_for_backup()
IS 'release the distributed write block held for backup';
