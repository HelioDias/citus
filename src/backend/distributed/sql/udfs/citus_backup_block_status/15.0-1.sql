CREATE OR REPLACE FUNCTION pg_catalog.citus_backup_block_status(
    OUT state text,
    OUT worker_pid int,
    OUT requestor_pid int,
    OUT block_start_time timestamptz,
    OUT timeout_ms int,
    OUT node_count int)
RETURNS record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_backup_block_status$$;
COMMENT ON FUNCTION pg_catalog.citus_backup_block_status()
IS 'return the current status of the distributed write block for backup';
