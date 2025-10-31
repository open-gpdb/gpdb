#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/dsa.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static file_create_hook_type prev_file_create_hook = NULL;
static file_unlink_hook_type prev_file_unlink_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

typedef struct TmpFile
{
	RelFileNode node;
	dsa_pointer next;
	dsa_pointer prev;
}	TmpFile;

typedef struct TmpFileList
{
	LWLock	   *lock;			/* protects the list */
	dsa_pointer head;			/* ptr to TmpFile list head */
}	TmpFileList;

typedef struct BackendsTmpFileArray
{
	TmpFileList *array;
	char		dsa_mem[FLEXIBLE_ARRAY_MEMBER];
}	BackendsTmpFileArray;

static BackendsTmpFileArray *backendsTmpFileArray = NULL;


/* Memory required for the BackendsTmpFileArray structure */
static inline Size
TFStructSize(void)
{
	return add_size(offsetof(BackendsTmpFileArray, dsa_mem),
					dsa_minimum_size());
}

/* Memory required for array of TmpFileList-s */
static inline Size
TFListArraySize(void)
{
	/* MaxBackends has not computed yet */
	return mul_size(sizeof(TmpFileList), MaxConnections);
}

/* Calculate shmem size for temporary tables files */
static Size
TFShmemSize(void)
{
	return add_size(TFStructSize(), TFListArraySize());
}

/*
 * Cleanup temp file list.
 * When the function is called, the list should be empty
 */
//static void
//shutdown_hook(int code, Datum arg)
//{
//	dsa_release_in_place(backendsTmpFileArray->dsa_mem);
//
//	if (MyBackendId == InvalidBackendId)
//		return;
//
//	TmpFileList *list = &backendsTmpFileArray->array[MyBackendId];
//
//	if (!DsaPointerIsValid(list->head))
//		return;
//
//	/* Assert on debug build and warning on release */
//	Assert(false);
//	ereport(WARNING,
//			(errcode(ERRCODE_INTERNAL_ERROR),
//			 errmsg("Temp file list is not empty. "
//					"MyBackend: %d, MyProcPid: %d", MyBackendId, MyProcPid)));
//	list->head = InvalidDsaPointer;
//}

/* Attach DSA once per process. */
static dsa_area *
TFAttachDsa(void)
{
	static dsa_area *dsa = NULL;	/* ptr to DSA area attached by
									 * current process */

	if (dsa)
		return dsa;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	dsa = dsa_attach_in_place(backendsTmpFileArray->dsa_mem, NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(dsa);

//	on_shmem_exit(shutdown_hook, 0);

	return dsa;
}


static void tts_file_create_hook(RelFileNodeBackend rnode)
{
	if (rnode.backend != TempRelBackendId)
		return;

	ereport(WARNING, (errmsg("create %s", relpath(rnode, MAIN_FORKNUM))));

	TmpFile *node;
	dsa_area   *dsa = TFAttachDsa();
	const dsa_pointer node_dsa = dsa_allocate(dsa, sizeof(*node));

	if (!DsaPointerIsValid(node_dsa))
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Not enough memory to add temp file node. "
				   "MyBackend: %d, MyProcPid: %d", MyBackendId, MyProcPid)));

	node = dsa_get_address(dsa, node_dsa);
	*node = (TmpFile)
	{
		.node = rnode.node,
		.prev = InvalidDsaPointer
	};

	TmpFileList *list = &backendsTmpFileArray->array[MyBackendId];

	LWLockAcquire(list->lock, LW_EXCLUSIVE);
	node->next = list->head;
	if (DsaPointerIsValid(node->next))
	{
		TmpFile *next_node = (TmpFile *) dsa_get_address(dsa, node->next);

		next_node->prev = node_dsa;
	}
	list->head = node_dsa;
	LWLockRelease(list->lock);
}

static void tts_file_unlink_hook(RelFileNodeBackend rnode)
{
	if (rnode.backend != TempRelBackendId)
		return;

	ereport(WARNING, (errmsg("unlink %s", relpath(rnode, MAIN_FORKNUM))));

	dsa_area   *dsa = TFAttachDsa();
	TmpFileList *list = &backendsTmpFileArray->array[MyBackendId];

	// Delete file from list
	LWLockAcquire(list->lock, LW_EXCLUSIVE);
	for (dsa_pointer node_dsa = list->head;DsaPointerIsValid(node_dsa);)
	{
		const TmpFile *node = dsa_get_address(dsa, node_dsa);

		if (node->node.dbNode  == rnode.node.dbNode &&
			node->node.relNode == rnode.node.relNode &&
			node->node.spcNode == rnode.node.spcNode)
		{
			if (DsaPointerIsValid(node->next))
			{
				TmpFile *next_node = dsa_get_address(dsa, node->next);

				next_node->prev = node->prev;
			}

			if (DsaPointerIsValid(node->prev))
			{
				TmpFile *prev_node = dsa_get_address(dsa, node->prev);

				prev_node->next = node->next;
			}
			else
				list->head = node->next;

			dsa_free(dsa, node_dsa);
			break;
		}
		node_dsa = node->next;
	}
	LWLockRelease(list->lock);
}

static void tts_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();

	bool		found;
	backendsTmpFileArray = (BackendsTmpFileArray *)
		ShmemInitStruct("temp_tables_stat array", TFStructSize(), &found);
	if (found)
		return;

	backendsTmpFileArray->array = (TmpFileList *)ShmemAlloc(TFListArraySize());
	if (backendsTmpFileArray->array == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Not enough memory to create temp_tables_stat lists.")));

	for (int i = 0; i < MaxConnections; i++)
		backendsTmpFileArray->array[i] = (TmpFileList)
		{
			.head = InvalidDsaPointer,
			.lock = LWLockAssign()
		};

	dsa_area   *dsa = dsa_create_in_place(
						 backendsTmpFileArray->dsa_mem, dsa_minimum_size(),
						 LWLockNewTrancheId(), "temp_tables_stat", NULL);

	on_shmem_exit(dsa_on_shmem_exit_release_in_place,
				  (Datum) backendsTmpFileArray->dsa_mem);
	dsa_detach(dsa);
}


/*
 * Get temp files list
 */
PG_FUNCTION_INFO_V1(tts_get_list);
Datum
tts_get_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		TupleDesc tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "path", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "size", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		dsa_area   *dsa = TFAttachDsa();
		RelFileNode *rfns = NULL;
		Size		size = 0;
		Size		step = 32;
		funcctx->max_calls = 0;

		for (int i = 0; i < MaxConnections; i++)
		{
			TmpFileList *list = &backendsTmpFileArray->array[i];

			LWLockAcquire(list->lock, LW_SHARED);

			for (dsa_pointer node_dsa = list->head;DsaPointerIsValid(node_dsa);)
			{
				const TmpFile *node = dsa_get_address(dsa, node_dsa);

				if (rfns == NULL)
				{
					size += step;
					rfns = palloc(sizeof(*rfns) * size);
				}
				else if (funcctx->max_calls >= size)
				{
					step *= 2;
					size += step;
					rfns = repalloc(rfns, sizeof(*rfns) * size);
				}

				rfns[funcctx->max_calls++] = node->node;
				node_dsa = node->next;
			}

			LWLockRelease(list->lock);
		}
		funcctx->user_fctx = rfns;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr >= funcctx->max_calls)
		SRF_RETURN_DONE(funcctx);

	RelFileNode *rfns = (RelFileNode*) funcctx->user_fctx;
	Datum		values[2] = {0};
	bool		nulls[2] = {0};

	values[0] = CStringGetTextDatum("dsadas");
	values[1] = Int64GetDatum(rfns[funcctx->call_cntr].relNode);

	HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(WARNING,
			(errmsg("temp_tables_stat is not in shared_preload_libraries")));
		return;
	}

	RequestAddinShmemSpace(TFShmemSize());
	RequestAddinLWLocks(MaxConnections);

	prev_file_create_hook = file_create_hook;
	file_create_hook      = tts_file_create_hook;

	prev_file_unlink_hook = file_unlink_hook;
	file_unlink_hook      = tts_file_unlink_hook;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook      = tts_shmem_startup;
}
