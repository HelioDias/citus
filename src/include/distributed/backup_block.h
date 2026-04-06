/*-------------------------------------------------------------------------
 *
 * backup_block.h
 *
 * Declarations for blocking distributed writes during LTR backup.
 *
 * The backup block feature allows external backup tools to temporarily
 * block distributed 2PC writes across the Citus cluster, take a
 * consistent snapshot on every node, and then release the block.
 *
 * Architecture:
 *   - A dedicated background worker holds the ExclusiveLocks on
 *     pg_dist_transaction / pg_dist_partition / pg_dist_node on
 *     coordinator + all worker nodes.
 *   - Shared memory (BackupBlockControlData) communicates state
 *     between the UDF caller, the background worker, and the
 *     status/unblock UDFs.
 *   - The background worker auto-releases if timeout expires,
 *     the requestor backend exits, or a worker connection fails.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKUP_BLOCK_H
#define BACKUP_BLOCK_H

#include "postgres.h"

#include "storage/lwlock.h"


/*
 * BackupBlockState enumerates the lifecycle states of the backup block.
 */
typedef enum BackupBlockState
{
	BACKUP_BLOCK_INACTIVE = 0,  /* no block active */
	BACKUP_BLOCK_STARTING,      /* background worker is starting up */
	BACKUP_BLOCK_ACTIVE,        /* locks acquired on all nodes */
	BACKUP_BLOCK_RELEASING,     /* unblock requested, releasing */
	BACKUP_BLOCK_ERROR          /* worker hit an error */
} BackupBlockState;


/*
 * BackupBlockControlData is the shared memory structure that coordinates
 * the backup block lifecycle between UDF callers and the background worker.
 *
 * Protected by the embedded LWLock (lock).
 */
typedef struct BackupBlockControlData
{
	/* LWLock tranche info */
	int trancheId;
	char lockTrancheName[NAMEDATALEN];
	LWLock lock;

	/* current state */
	BackupBlockState state;

	/* PIDs for lifecycle management */
	pid_t workerPid;           /* PID of the background worker holding locks */
	pid_t requestorPid;        /* PID of the backend that requested the block */

	/* timing */
	TimestampTz blockStartTime;  /* when locks were acquired */
	int timeoutMs;               /* auto-release timeout in milliseconds */

	/* cluster info */
	int nodeCount;             /* number of worker nodes locked */

	/* error reporting */
	char errorMessage[256];    /* error message if state == BACKUP_BLOCK_ERROR */

	/* signal: set to true by unblock UDF to tell worker to release */
	bool releaseRequested;
} BackupBlockControlData;


/*
 * BLOCK_DISTRIBUTED_WRITES_COMMAND acquires ExclusiveLock on:
 *   1. pg_dist_transaction — blocks 2PC commit decisions
 *   2. pg_dist_partition   — blocks DDL on distributed tables
 *
 * Used by both citus_block_writes_for_backup and citus_create_restore_point
 * to quiesce distributed writes on remote metadata nodes.
 *
 * Note: pg_dist_node is only locked locally on the coordinator (node
 * management operations are coordinator-only), so it is intentionally
 * absent from this remote command.
 */
#define BLOCK_DISTRIBUTED_WRITES_COMMAND \
	"LOCK TABLE pg_catalog.pg_dist_transaction IN EXCLUSIVE MODE; " \
	"LOCK TABLE pg_catalog.pg_dist_partition IN EXCLUSIVE MODE"


/* Shared memory sizing and initialization */
extern size_t BackupBlockShmemSize(void);
extern void BackupBlockShmemInit(void);

/* Call from _PG_init to chain shmem_startup_hook */
extern void InitializeBackupBlock(void);

/* Background worker entry point */
extern PGDLLEXPORT void CitusBackupBlockWorkerMain(Datum main_arg);

#endif /* BACKUP_BLOCK_H */
