/*-------------------------------------------------------------------------
 *
 * backup_block.c
 *
 * Implementation of UDFs for blocking distributed writes during LTR backup.
 *
 * This file provides three SQL-callable functions:
 *
 *   citus_block_writes_for_backup(timeout_ms int DEFAULT 300000)
 *     -> Spawns a background worker that acquires ExclusiveLock on
 *        pg_dist_transaction, pg_dist_partition, and pg_dist_node on
 *        the coordinator and all worker nodes.  Returns true when
 *        locks are held.
 *
 *   citus_unblock_writes_for_backup()
 *     -> Signals the background worker to release all locks and exit.
 *        Returns true on success.
 *
 *   citus_backup_block_status()
 *     -> Returns a single-row result describing the current block state:
 *        (state text, worker_pid int, requestor_pid int,
 *         block_start_time timestamptz, timeout_ms int, node_count int)
 *
 * Architecture:
 *   The actual lock-holding is done by a dedicated background worker
 *   (CitusBackupBlockWorkerMain).  This ensures locks survive the
 *   caller's session disconnect.  The background worker communicates
 *   its state through shared memory (BackupBlockControlData).
 *
 *   The worker auto-releases locks when:
 *     - citus_unblock_writes_for_backup() sets releaseRequested
 *     - The configured timeout expires
 *     - A worker-node connection fails
 *     - The postmaster dies
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "distributed/backup_block.h"
#include "distributed/background_worker_utils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"


/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(citus_block_writes_for_backup);
PG_FUNCTION_INFO_V1(citus_unblock_writes_for_backup);
PG_FUNCTION_INFO_V1(citus_backup_block_status);


/* default and maximum timeout for backup block (5 minutes / 30 minutes) */
#define BACKUP_BLOCK_DEFAULT_TIMEOUT_MS  300000
#define BACKUP_BLOCK_MAX_TIMEOUT_MS      1800000

/* polling interval while waiting for worker to acquire locks */
#define BACKUP_BLOCK_POLL_INTERVAL_MS    100

/* interval for worker to check latch / release conditions */
#define BACKUP_BLOCK_WORKER_CHECK_MS     1000

/* maximum iterations to wait for unblock completion: 300 * 100ms = 30s */
#define BACKUP_BLOCK_UNBLOCK_MAX_LOOPS   300


/* shared memory pointer */
static BackupBlockControlData *BackupBlockControl = NULL;

/* previous shmem_startup_hook */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* SIGTERM flag for worker */
static volatile sig_atomic_t got_sigterm = false;


/* forward declarations */
static List * OpenConnectionsToMetadataWorkersForBlock(LOCKMODE lockMode);
static void BlockDistributedTransactionsLocally(void);
static void BlockDistributedTransactionsOnWorkers(List *connectionList,
												  int timeoutMs,
												  int *nodeCount);
static void backup_block_worker_sigterm(SIGNAL_ARGS);
static void SetBackupBlockState(BackupBlockState newState);
static void SetBackupBlockError(const char *message);


/* ----------------------------------------------------------------
 * Shared Memory
 * ---------------------------------------------------------------- */

/*
 * BackupBlockShmemSize returns the amount of shared memory needed for
 * the backup block control structure.
 */
size_t
BackupBlockShmemSize(void)
{
	return sizeof(BackupBlockControlData);
}


/*
 * BackupBlockShmemInit initializes the shared memory for backup block.
 * Called from citus_shmem_startup_hook or similar.
 */
void
BackupBlockShmemInit(void)
{
	bool alreadyInitialized = false;

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	BackupBlockControl =
		(BackupBlockControlData *) ShmemInitStruct("Citus Backup Block",
												   BackupBlockShmemSize(),
												   &alreadyInitialized);

	if (!alreadyInitialized)
	{
		BackupBlockControl->trancheId = LWLockNewTrancheId();
		strlcpy(BackupBlockControl->lockTrancheName, "Citus Backup Block",
				NAMEDATALEN);
		LWLockRegisterTranche(BackupBlockControl->trancheId,
							  BackupBlockControl->lockTrancheName);
		LWLockInitialize(&BackupBlockControl->lock,
						 BackupBlockControl->trancheId);

		BackupBlockControl->state = BACKUP_BLOCK_INACTIVE;
		BackupBlockControl->workerPid = 0;
		BackupBlockControl->requestorPid = 0;
		BackupBlockControl->blockStartTime = 0;
		BackupBlockControl->timeoutMs = 0;
		BackupBlockControl->nodeCount = 0;
		BackupBlockControl->errorMessage[0] = '\0';
		BackupBlockControl->releaseRequested = false;
	}

	LWLockRelease(AddinShmemInitLock);
}


/*
 * InitializeBackupBlock installs the shmem_startup_hook to initialize
 * backup block shared memory.  Called from _PG_init.
 */
void
InitializeBackupBlock(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = BackupBlockShmemInit;
}


/* ----------------------------------------------------------------
 * Helper: State management (caller must NOT hold LWLock)
 * ---------------------------------------------------------------- */

static void
SetBackupBlockState(BackupBlockState newState)
{
	LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
	BackupBlockControl->state = newState;
	LWLockRelease(&BackupBlockControl->lock);
}


static void
SetBackupBlockError(const char *message)
{
	LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
	BackupBlockControl->state = BACKUP_BLOCK_ERROR;
	strlcpy(BackupBlockControl->errorMessage, message,
			sizeof(BackupBlockControl->errorMessage));
	LWLockRelease(&BackupBlockControl->lock);
}


/* ----------------------------------------------------------------
 * Background Worker: CitusBackupBlockWorkerMain
 *
 * Lifecycle:
 *   1. Connect to database
 *   2. Open connections to metadata worker nodes
 *   3. Acquire local ExclusiveLocks (coordinator)
 *   4. Send LOCK TABLE commands to all metadata workers
 *   5. Update shared memory -> BACKUP_BLOCK_ACTIVE
 *   6. Wait loop: check latch for release / timeout / postmaster death
 *   7. Close remote connections (rolls back remote transactions,
 *      releasing remote locks)
 *   8. Exit (local locks released when transaction ends)
 * ---------------------------------------------------------------- */

/*
 * Signal handler for SIGTERM in the background worker.
 */
static void
backup_block_worker_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * CitusBackupBlockWorkerMain is the entry point for the backup block
 * background worker.  It acquires locks on the coordinator and all
 * worker nodes, then holds them until told to release.
 */
void
CitusBackupBlockWorkerMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);

	/*
	 * Extract timeout from bgw_extra.
	 * Layout: [Oid extensionOwner][int timeoutMs]
	 * Always populated by RegisterCitusBackgroundWorker.
	 */
	int timeoutMs = BACKUP_BLOCK_DEFAULT_TIMEOUT_MS;
	memcpy(&timeoutMs, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(int));

	pqsignal(SIGTERM, backup_block_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* connect to database */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, InvalidOid, 0);

	elog(LOG, "backup block worker started (timeout=%dms)", timeoutMs);

	/* start a transaction — locks live until this transaction ends */
	StartTransactionCommand();

	List *connectionList = NIL;

	PG_TRY();
	{
		/* open connections to metadata worker nodes */
		connectionList = OpenConnectionsToMetadataWorkersForBlock(ShareLock);

		/* begin remote transactions (to bust through pgbouncer) */
		RemoteTransactionListBegin(connectionList);

		/* acquire local locks on coordinator */
		BlockDistributedTransactionsLocally();

		/* acquire remote locks on all metadata workers */
		int nodeCount = 0;
		BlockDistributedTransactionsOnWorkers(connectionList, timeoutMs,
											 &nodeCount);

		/* update shared memory: locks acquired */
		LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
		BackupBlockControl->state = BACKUP_BLOCK_ACTIVE;
		BackupBlockControl->blockStartTime = GetCurrentTimestamp();
		BackupBlockControl->nodeCount = nodeCount;
		BackupBlockControl->workerPid = MyProcPid;
		LWLockRelease(&BackupBlockControl->lock);

		elog(LOG, "backup block active: locks held on coordinator + %d worker nodes",
			 nodeCount);

		/*
		 * Build a WaitEventSet that monitors:
		 *   - Each remote connection socket (detect dead workers immediately)
		 *   - Latch (for release request / SIGTERM wakeup)
		 *   - Postmaster death
		 *
		 * Slot count: one per connection + latch + postmaster death.
		 */
		int eventSetSize = list_length(connectionList) + 2;
		WaitEventSet *waitEventSet =
			CreateWaitEventSet(WaitEventSetTracker_compat, eventSetSize);

		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			int sock = PQsocket(conn->pgConn);
			if (sock == -1)
			{
				FreeWaitEventSet(waitEventSet);
				ereport(ERROR,
						(errmsg("backup block: remote connection already "
								"closed before entering wait loop")));
			}

			int idx = CitusAddWaitEventSetToSet(waitEventSet,
												WL_SOCKET_READABLE,
												sock, NULL,
												(void *) conn);
			if (idx == WAIT_EVENT_SET_INDEX_FAILED)
			{
				FreeWaitEventSet(waitEventSet);
				ereport(ERROR,
						(errmsg("backup block: could not add remote socket "
								"to wait event set")));
			}
		}
		AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH,
						  PGINVALID_SOCKET, NULL, NULL);
		AddWaitEventToSet(waitEventSet, WL_LATCH_SET,
						  PGINVALID_SOCKET, MyLatch, NULL);

		/*
		 * Hold locks until release is requested, timeout expires,
		 * a remote connection fails, or we receive SIGTERM / postmaster death.
		 */
		TimestampTz startTime = GetCurrentTimestamp();
		WaitEvent *events = palloc0(eventSetSize * sizeof(WaitEvent));

		while (!got_sigterm)
		{
			int eventCount = WaitEventSetWait(waitEventSet,
											  BACKUP_BLOCK_WORKER_CHECK_MS,
											  events, eventSetSize,
											  PG_WAIT_EXTENSION);

			for (int i = 0; i < eventCount; i++)
			{
				WaitEvent *event = &events[i];

				if (event->events & WL_POSTMASTER_DEATH)
				{
					FreeWaitEventSet(waitEventSet);
					pfree(events);
					SetBackupBlockError(
						"postmaster died while holding backup block");
					proc_exit(1);
				}

				if (event->events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);
					continue;
				}

				if (event->events & WL_SOCKET_READABLE)
				{
					/*
					 * An idle locked connection should never receive data.
					 * Any readability means the remote end sent something
					 * unexpected (e.g. termination notice) or the socket
					 * is broken.
					 */
					MultiConnection *failedConn =
						(MultiConnection *) event->user_data;
					bool connectionBad = false;

					if (PQconsumeInput(failedConn->pgConn) == 0)
					{
						connectionBad = true;
					}
					else if (PQstatus(failedConn->pgConn) == CONNECTION_BAD)
					{
						connectionBad = true;
					}

					if (connectionBad)
					{
						FreeWaitEventSet(waitEventSet);
						pfree(events);
						ereport(ERROR,
								(errmsg("backup block: lost connection to "
										"worker node %s:%d",
										failedConn->hostname,
										failedConn->port)));
					}
				}
			}

			CHECK_FOR_INTERRUPTS();

			/* check if release was requested */
			bool shouldRelease = false;
			LWLockAcquire(&BackupBlockControl->lock, LW_SHARED);
			shouldRelease = BackupBlockControl->releaseRequested;
			LWLockRelease(&BackupBlockControl->lock);

			if (shouldRelease)
			{
				SetBackupBlockState(BACKUP_BLOCK_RELEASING);
				elog(LOG, "backup block: release requested, shutting down");
				break;
			}

			/* check timeout */
			TimestampTz now = GetCurrentTimestamp();
			long elapsedMs = (long) ((now - startTime) / 1000);
			if (timeoutMs > 0 && elapsedMs >= timeoutMs)
			{
				elog(WARNING, "backup block: timeout reached (%d ms), auto-releasing",
					 timeoutMs);
				break;
			}
		}

		FreeWaitEventSet(waitEventSet);
		pfree(events);

		/* clean up: close remote connections (rolls back remote transactions) */
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}
		connectionList = NIL;
	}
	PG_CATCH();
	{
		/* on error, clean up connections and report */
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}

		/*
		 * SIGTERM-induced errors are controlled shutdowns (e.g. postmaster
		 * restart), not real failures — report INACTIVE instead of ERROR
		 * so the state is clean for the next startup.
		 */
		if (got_sigterm)
		{
			SetBackupBlockState(BACKUP_BLOCK_INACTIVE);
		}
		else
		{
			SetBackupBlockError(edata->message);
		}

		FreeErrorData(edata);

		AbortCurrentTransaction();

		elog(LOG, "backup block worker exiting due to %s",
			 got_sigterm ? "SIGTERM" : "error");
		proc_exit(got_sigterm ? 0 : 1);
	}
	PG_END_TRY();

	/* abort transaction to release local locks */
	AbortCurrentTransaction();

	/* mark shared memory as inactive */
	LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
	BackupBlockControl->state = BACKUP_BLOCK_INACTIVE;
	BackupBlockControl->workerPid = 0;
	BackupBlockControl->requestorPid = 0;
	BackupBlockControl->releaseRequested = false;
	BackupBlockControl->nodeCount = 0;
	BackupBlockControl->errorMessage[0] = '\0';
	BackupBlockControl->blockStartTime = 0;
	BackupBlockControl->timeoutMs = 0;
	LWLockRelease(&BackupBlockControl->lock);

	elog(LOG, "backup block worker finished, all locks released");
	proc_exit(0);
}


/* ----------------------------------------------------------------
 * Lock acquisition helpers
 * ---------------------------------------------------------------- */

/*
 * OpenConnectionsToMetadataWorkersForBlock opens connections to all
 * remote metadata worker nodes using FORCE_NEW_CONNECTION.
 *
 * Unlike citus_create_restore_point (which needs all nodes for
 * pg_create_restore_point), backup block only needs metadata nodes
 * for the LOCK TABLE commands.
 */
static List *
OpenConnectionsToMetadataWorkersForBlock(LOCKMODE lockMode)
{
	List *connectionList = NIL;
	int connectionFlags = FORCE_NEW_CONNECTION;

	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(lockMode);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		if (!NodeIsPrimaryAndRemote(workerNode) || !workerNode->hasMetadata)
		{
			continue;
		}

		MultiConnection *connection = StartNodeConnection(connectionFlags,
														  workerNode->workerName,
														  workerNode->workerPort);
		MarkRemoteTransactionCritical(connection);
		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	return connectionList;
}


/*
 * BlockDistributedTransactionsLocally acquires ExclusiveLock on
 * pg_dist_node, pg_dist_partition, and pg_dist_transaction on the
 * coordinator.  Same pattern as citus_create_restore_point.
 */
static void
BlockDistributedTransactionsLocally(void)
{
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);
	LockRelationOid(DistPartitionRelationId(), ExclusiveLock);
	LockRelationOid(DistTransactionRelationId(), ExclusiveLock);
}


/*
 * BlockDistributedTransactionsOnWorkers sends LOCK TABLE commands to
 * all connections (which are already filtered to metadata workers)
 * to acquire ExclusiveLocks remotely.
 *
 * A statement_timeout is set on each connection so that lock acquisition
 * does not hang indefinitely if a remote node is unresponsive.
 *
 * Sets *nodeCount to the number of nodes locked.
 */
static void
BlockDistributedTransactionsOnWorkers(List *connectionList, int timeoutMs,
									  int *nodeCount)
{
	/* set statement_timeout on each remote connection to bound lock wait */
	char timeoutCommand[128];
	SafeSnprintf(timeoutCommand, sizeof(timeoutCommand),
				 "SET LOCAL statement_timeout = %d", timeoutMs);

	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, timeoutCommand);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach_declared_ptr(connection, connectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}
		PQclear(result);
		ForgetResults(connection);
	}

	/* send lock commands in parallel */
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection,
											  BLOCK_DISTRIBUTED_WRITES_COMMAND);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* wait for all lock acquisitions */
	foreach_declared_ptr(connection, connectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}
		PQclear(result);
		ForgetResults(connection);
	}

	*nodeCount = list_length(connectionList);
}


/* ----------------------------------------------------------------
 * SQL-callable UDFs
 * ---------------------------------------------------------------- */

/*
 * citus_block_writes_for_backup blocks distributed 2PC writes across
 * the entire Citus cluster.  A background worker is spawned to hold
 * the locks, so they survive if this session disconnects.
 *
 * Parameters:
 *   timeout_ms (int, default 300000) - auto-release after this many ms
 *
 * Returns: true when locks are held on all nodes.
 */
Datum
citus_block_writes_for_backup(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	int timeoutMs = PG_GETARG_INT32(0);

	if (timeoutMs <= 0 || timeoutMs > BACKUP_BLOCK_MAX_TIMEOUT_MS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timeout_ms must be between 1 and %d",
						BACKUP_BLOCK_MAX_TIMEOUT_MS)));
	}

	/*
	 * Atomically check-and-set under a single LW_EXCLUSIVE acquisition
	 * to prevent TOCTOU races between concurrent callers.
	 */
	LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
	BackupBlockState currentState = BackupBlockControl->state;

	if (currentState == BACKUP_BLOCK_ACTIVE ||
		currentState == BACKUP_BLOCK_STARTING)
	{
		LWLockRelease(&BackupBlockControl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a backup block is already active"),
				 errhint("Use citus_unblock_writes_for_backup() to release "
						 "the existing block first.")));
	}

	BackupBlockControl->state = BACKUP_BLOCK_STARTING;
	BackupBlockControl->requestorPid = MyProcPid;
	BackupBlockControl->workerPid = 0;
	BackupBlockControl->releaseRequested = false;
	BackupBlockControl->errorMessage[0] = '\0';
	BackupBlockControl->nodeCount = 0;
	BackupBlockControl->timeoutMs = timeoutMs;
	LWLockRelease(&BackupBlockControl->lock);

	/* spawn the background worker */
	char workerName[BGW_MAXLEN];
	SafeSnprintf(workerName, BGW_MAXLEN,
				 "Citus Backup Block Worker: %u", MyDatabaseId);

	CitusBackgroundWorkerConfig config = {
		.workerName = workerName,
		.functionName = "CitusBackupBlockWorkerMain",
		.mainArg = ObjectIdGetDatum(MyDatabaseId),
		.extensionOwner = CitusExtensionOwner(),
		.needsNotification = true,
		.waitForStartup = true,
		.restartTime = CITUS_BGW_NEVER_RESTART,
		.startTime = BgWorkerStart_RecoveryFinished,
		.workerType = "citus_backup_block",
		.extraData = &timeoutMs,
		.extraDataSize = sizeof(int)
	};

	BackgroundWorkerHandle *handle = RegisterCitusBackgroundWorker(&config);
	if (!handle)
	{
		SetBackupBlockState(BACKUP_BLOCK_INACTIVE);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start backup block background worker"),
				 errhint("Check that max_worker_processes is high enough.")));
	}

	/*
	 * Poll shared memory until the worker reports ACTIVE or ERROR.
	 * The worker was started with waitForStartup=true, so it's
	 * already running at this point.
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&BackupBlockControl->lock, LW_SHARED);
		BackupBlockState state = BackupBlockControl->state;
		char errMsg[256];
		if (state == BACKUP_BLOCK_ERROR)
		{
			strlcpy(errMsg, BackupBlockControl->errorMessage, sizeof(errMsg));
		}
		LWLockRelease(&BackupBlockControl->lock);

		if (state == BACKUP_BLOCK_ACTIVE)
		{
			break;
		}

		if (state == BACKUP_BLOCK_ERROR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("backup block worker failed: %s", errMsg)));
		}

		if (state == BACKUP_BLOCK_INACTIVE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("backup block worker exited unexpectedly")));
		}

		/* still STARTING, check if worker is still alive */
		pid_t bgwPid;
		BgwHandleStatus bgwStatus = GetBackgroundWorkerPid(handle, &bgwPid);
		if (bgwStatus == BGWH_STOPPED)
		{
			SetBackupBlockState(BACKUP_BLOCK_INACTIVE);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("backup block worker exited unexpectedly "
							 "(crashed or killed)")));
		}

		/* still STARTING, wait a bit */
		int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   BACKUP_BLOCK_POLL_INTERVAL_MS,
						   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}
	}

	PG_RETURN_BOOL(true);
}


/*
 * citus_unblock_writes_for_backup signals the background worker to
 * release all locks and exit.  Can be called from any session.
 *
 * Returns: true if the block was released, false if no block was active.
 */
Datum
citus_unblock_writes_for_backup(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);
	BackupBlockState currentState = BackupBlockControl->state;

	if (currentState != BACKUP_BLOCK_ACTIVE &&
		currentState != BACKUP_BLOCK_STARTING)
	{
		LWLockRelease(&BackupBlockControl->lock);
		PG_RETURN_BOOL(false);
	}

	BackupBlockControl->releaseRequested = true;

	/* wake the worker via its latch if it has a valid PID */
	pid_t workerPid = BackupBlockControl->workerPid;
	LWLockRelease(&BackupBlockControl->lock);

	if (workerPid > 0)
	{
		/*
		 * Send SIGUSR1 to wake the worker's latch so it checks
		 * releaseRequested and exits cleanly via the release path.
		 * This is gentler than SIGTERM and allows the worker to
		 * transition through BACKUP_BLOCK_RELEASING state.
		 */
		kill(workerPid, SIGUSR1);
	}

	/*
	 * Wait for the worker to finish releasing.
	 * Poll shared memory with a short interval.
	 */
	for (int i = 0; i < BACKUP_BLOCK_UNBLOCK_MAX_LOOPS; i++)
	{
		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&BackupBlockControl->lock, LW_SHARED);
		BackupBlockState state = BackupBlockControl->state;
		LWLockRelease(&BackupBlockControl->lock);

		if (state == BACKUP_BLOCK_INACTIVE)
		{
			PG_RETURN_BOOL(true);
		}

		int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   BACKUP_BLOCK_POLL_INTERVAL_MS,
						   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}
	}

	ereport(WARNING,
			(errmsg("backup block worker did not shut down within 30 seconds")));

	PG_RETURN_BOOL(false);
}


/*
 * citus_backup_block_status returns a single row describing the current
 * state of the backup write block.
 *
 * Returns: (state text, worker_pid int, requestor_pid int,
 *           block_start_time timestamptz, timeout_ms int, node_count int)
 */
Datum
citus_backup_block_status(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TupleDesc tupleDesc;

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	tupleDesc = BlessTupleDesc(tupleDesc);

	Datum values[6];
	bool nulls[6];
	memset(nulls, 0, sizeof(nulls));

	LWLockAcquire(&BackupBlockControl->lock, LW_SHARED);

	BackupBlockState currentState = BackupBlockControl->state;
	pid_t workerPid = BackupBlockControl->workerPid;

	LWLockRelease(&BackupBlockControl->lock);

	/*
	 * Detect stale state: if the worker is supposed to be alive but its
	 * process no longer exists (SIGKILL, OOM-killer, etc.), auto-clean
	 * the shared memory so the system doesn't get permanently stuck.
	 */
	if ((currentState == BACKUP_BLOCK_ACTIVE ||
		 currentState == BACKUP_BLOCK_STARTING ||
		 currentState == BACKUP_BLOCK_RELEASING) &&
		workerPid > 0 &&
		kill(workerPid, 0) == -1 && errno == ESRCH)
	{
		/* worker is gone — re-acquire exclusive and double-check */
		LWLockAcquire(&BackupBlockControl->lock, LW_EXCLUSIVE);

		if ((BackupBlockControl->state == BACKUP_BLOCK_ACTIVE ||
			 BackupBlockControl->state == BACKUP_BLOCK_STARTING ||
			 BackupBlockControl->state == BACKUP_BLOCK_RELEASING) &&
			BackupBlockControl->workerPid == workerPid)
		{
			elog(WARNING, "backup block: detected stale state (worker PID %d "
				 "no longer exists), auto-cleaning", workerPid);

			BackupBlockControl->state = BACKUP_BLOCK_INACTIVE;
			BackupBlockControl->workerPid = 0;
			BackupBlockControl->requestorPid = 0;
			BackupBlockControl->releaseRequested = false;
			BackupBlockControl->nodeCount = 0;
			BackupBlockControl->errorMessage[0] = '\0';
			BackupBlockControl->blockStartTime = 0;
			BackupBlockControl->timeoutMs = 0;
		}

		currentState = BackupBlockControl->state;
		LWLockRelease(&BackupBlockControl->lock);
	}

	LWLockAcquire(&BackupBlockControl->lock, LW_SHARED);

	/* state */
	const char *stateStr;
	switch (BackupBlockControl->state)
	{
		case BACKUP_BLOCK_INACTIVE:
			stateStr = "inactive";
			break;

		case BACKUP_BLOCK_STARTING:
			stateStr = "starting";
			break;

		case BACKUP_BLOCK_ACTIVE:
			stateStr = "active";
			break;

		case BACKUP_BLOCK_RELEASING:
			stateStr = "releasing";
			break;

		case BACKUP_BLOCK_ERROR:
			stateStr = "error";
			break;

		default:
			stateStr = "unknown";
			break;
	}
	values[0] = CStringGetTextDatum(stateStr);

	/* worker_pid */
	if (BackupBlockControl->workerPid > 0)
	{
		values[1] = Int32GetDatum(BackupBlockControl->workerPid);
	}
	else
	{
		nulls[1] = true;
	}

	/* requestor_pid */
	if (BackupBlockControl->requestorPid > 0)
	{
		values[2] = Int32GetDatum(BackupBlockControl->requestorPid);
	}
	else
	{
		nulls[2] = true;
	}

	/* block_start_time */
	if (BackupBlockControl->blockStartTime > 0)
	{
		values[3] = TimestampTzGetDatum(BackupBlockControl->blockStartTime);
	}
	else
	{
		nulls[3] = true;
	}

	/* timeout_ms */
	values[4] = Int32GetDatum(BackupBlockControl->timeoutMs);

	/* node_count */
	values[5] = Int32GetDatum(BackupBlockControl->nodeCount);

	LWLockRelease(&BackupBlockControl->lock);

	HeapTuple tuple = heap_form_tuple(tupleDesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
