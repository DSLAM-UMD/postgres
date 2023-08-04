/* contrib/remotexact/remotexact.c */
#include "postgres.h"

#include "access/csn_snapshot.h"
#include "access/xact.h"
#include "access/remotexact.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
#include "rwset.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUCs */
char	   *remotexact_connstring;

typedef struct CollectedRelationKey
{
	Oid			relid;
} CollectedRelationKey;

typedef struct CollectedRelation
{
	CollectedRelationKey key;

	int8		region;
	bool		is_index;
	int			nitems;

	StringInfoData pages;
	StringInfoData tuples;
} CollectedRelation;

typedef struct RWSetCollectionBuffer
{
	MemoryContext context;

	RWSetHeader header;
	HTAB	   *collected_relations;
	StringInfoData writes;
} RWSetCollectionBuffer;

static RWSetCollectionBuffer *rwset_collection_buffer = NULL;

PGconn	   *XactServerConn;
bool		Connected = false;

static void init_rwset_collection_buffer(Oid dbid);
static void rwset_add_region(int region);

static void rx_collect_region(Relation relation);
static void rx_collect_relation(Oid dbid, Oid relid);
static void rx_collect_page(Oid dbid, Oid relid, BlockNumber blkno);
static void rx_collect_tuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber tid);
static void rx_collect_insert(Relation relation, HeapTuple newtuple);
static void rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
static void rx_collect_delete(Relation relation, HeapTuple oldtuple);
static void rx_clear_rwset_collection_buffer(void);
static void rx_send_rwset_and_wait(void);

static CollectedRelation *get_collected_relation(Oid relid, bool create_if_not_found);
static bool connect_to_txn_server(void);

static void
init_rwset_collection_buffer(Oid dbid)
{
	MemoryContext old_context;
	HASHCTL		hash_ctl;
	Snapshot 	snapshot;

	if (rwset_collection_buffer)
	{
		Oid old_dbid = rwset_collection_buffer->header.dbid;

		if (old_dbid != dbid)
			ereport(ERROR,
					errmsg("[remotexact] Remotexact can access only one database"),
					errdetail("old dbid: %u, new dbid: %u", old_dbid, dbid));
		return;
	}

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	rwset_collection_buffer = (RWSetCollectionBuffer *) palloc(sizeof(RWSetCollectionBuffer));
	rwset_collection_buffer->context = TopTransactionContext;

	/* Initialize the header */
	rwset_collection_buffer->header.dbid = dbid;
	rwset_collection_buffer->header.xid = InvalidTransactionId;
	snapshot = GetLatestSnapshot();
	rwset_collection_buffer->header.csn = snapshot->snapshot_csn;
	/* The current region is always a participant of the transaction */
	rwset_collection_buffer->header.region_set = UINT64CONST(1) << current_region;

	/* Initialize a map from relation oid to the read set of the relation */
	hash_ctl.hcxt = rwset_collection_buffer->context;
	hash_ctl.keysize = sizeof(CollectedRelationKey);
	hash_ctl.entrysize = sizeof(CollectedRelation);
	rwset_collection_buffer->collected_relations = hash_create("collected relations",
															   max_predicate_locks_per_xact,
															   &hash_ctl,
															   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Initialize the buffer for the write set */
	initStringInfo(&rwset_collection_buffer->writes);

	MemoryContextSwitchTo(old_context);
}

static void
rwset_add_region(int region)
{
	Assert(RegionIsValid(region));
	Assert(rwset_collection_buffer != NULL);

	/* Set the corresponding region bit in the header */
	rwset_collection_buffer->header.region_set |= UINT64CONST(1) << region;
}

static void
rx_collect_region(Relation relation)
{
	int	region = RelationGetRegion(relation);
	int	max_nregions = BITS_PER_BYTE * sizeof(uint64);
	CollectedRelation *crel;

	if (region < 0 || region >= max_nregions)
		ereport(ERROR,
				errmsg("[remotexact] Region id is out of bound"),
				errdetail("region id: %u, min: 0, max: %u", region, max_nregions));

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	crel = get_collected_relation(RelationGetRelid(relation), false);
	Assert(crel != NULL);

	/* Set the region for the individual relation */
	crel->region = region;

	rwset_add_region(region);
}

static void
rx_collect_relation(Oid dbid, Oid relid)
{
	CollectedRelation *collected_relation;

	init_rwset_collection_buffer(dbid);
	collected_relation = get_collected_relation(relid, true);
	collected_relation->is_index = false;
}

static void
rx_collect_page(Oid dbid, Oid relid, BlockNumber blkno)
{
	CollectedRelation *collected_relation;
	StringInfo	buf = NULL;
	Snapshot snapshot;

	init_rwset_collection_buffer(dbid);

	collected_relation = get_collected_relation(relid, true);
	collected_relation->is_index = true;
	collected_relation->nitems++;

	snapshot = GetLatestSnapshot();

	buf = &collected_relation->pages;
	pq_sendint32(buf, blkno);
	pq_sendint64(buf, snapshot->snapshot_csn);
}

static void
rx_collect_tuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset)
{
	CollectedRelation *collected_relation;
	StringInfo	buf = NULL;

	init_rwset_collection_buffer(dbid);

	collected_relation = get_collected_relation(relid, true);
	collected_relation->is_index = false;
	collected_relation->nitems++;

	buf = &collected_relation->tuples;
	pq_sendint32(buf, blkno);
	pq_sendint16(buf, offset);
}

static void
rx_collect_insert(Relation relation, HeapTuple newtuple)
{
	int region = RelationGetRegion(relation);
	StringInfo	buf = NULL;
	TupleTableSlot *newslot;

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	buf = &rwset_collection_buffer->writes;
	// TODO (ctring): logicalrep_write_insert in postgres 15 requires a tuple slot,
	//				  so creating it here to make things compile for now. This
	//				  might not be efficient and should be revised.
	newslot = MakeTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(newtuple, newslot, false);

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);
	/* Encode the insert using the logical replication protocol */
	logicalrep_write_insert(buf,
							InvalidTransactionId,
							relation,
							newslot,
							true /* binary */,
							NULL /* columns */);

	rwset_add_region(region);
}

static void
rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple)
{
	StringInfo	buf = NULL;
	TupleTableSlot *oldslot;
	TupleTableSlot *newslot;

	char		relreplident = relation->rd_rel->relreplident;
	int			region = RelationGetRegion(relation);

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	// TOOD (ctring): We need to set the replica identity to something other than NOTHING
	// to collect the write set. Need to figure out a way to get rid of this step
	// or a check to prevent us from forgetting to do this step.
	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
		return;

	buf = &rwset_collection_buffer->writes;
	// TODO (ctring): logicalrep_write_update in postgres 15 requires tuple slots,
	//				  so creating them here to make things compile for now. This
	//				  might not be efficient and should be revised.
	oldslot = MakeTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(oldtuple, oldslot, false);
	newslot = MakeTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(newtuple, newslot, false);

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);
	/* Encode the update using the logical replication protocol */
	logicalrep_write_update(buf,
							InvalidTransactionId,
							relation,
							oldslot,
							newslot,
							true /* binary */,
							NULL /* columns */);

	rwset_add_region(region);
}

static void
rx_collect_delete(Relation relation, HeapTuple oldtuple)
{
	int			region = RelationGetRegion(relation);
	StringInfo	buf = NULL;
	char		relreplident = relation->rd_rel->relreplident;

	TupleTableSlot *oldslot;

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	// TOOD (ctring): We need to set the replica identity to something other than NOTHING
	// to collect the write set. Need to figure out a way to get rid of this step
	// or a check to prevent us from forgetting to do this step.
	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
		return;

	if (oldtuple == NULL)
		return;

	buf = &rwset_collection_buffer->writes;
	// TODO (ctring): logicalrep_write_delete in postgres 15 requires a tuple slot,
	//				  so creating it here to make things compile for now. This
	//				  might not be efficient and should be revised.
	oldslot = MakeTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(oldtuple, oldslot, false);

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);
	/* Encode the delete using the logical replication protocol */
	logicalrep_write_delete(buf, InvalidTransactionId, relation, oldslot, true /* binary */);

	rwset_add_region(region);
}


static void
rx_clear_rwset_collection_buffer(void)
{
	rwset_collection_buffer = NULL;
}

static void
rx_send_rwset_and_wait(void)
{
	RWSet	   *rwset;
	RWSetHeader *header;
	CollectedRelation *collected_relation;
	HASH_SEQ_STATUS status;
	int			read_len = 0;
	StringInfoData buf;

	if (rwset_collection_buffer == NULL)
		return;

	if (!connect_to_txn_server())
		return;

	initStringInfo(&buf);

	/* Assemble the header */
	header = &rwset_collection_buffer->header;
	pq_sendint32(&buf, header->dbid);
	pq_sendint32(&buf, header->xid);
	pq_sendint64(&buf, header->csn);
	pq_sendint64(&buf, header->region_set);

	/* Cursor now points to where the length of the read section is stored */
	buf.cursor = buf.len;
	/* Read section length will be updated later */
	pq_sendint32(&buf, 0);

	/* Assemble the read set */
	hash_seq_init(&status, rwset_collection_buffer->collected_relations);
	while ((collected_relation = (CollectedRelation *) hash_seq_search(&status)) != NULL)
	{
		StringInfo	items = NULL;

		/* Accumulate the length of the buffer used by each relation */
		read_len -= buf.len;

		if (collected_relation->is_index)
		{
			pq_sendbyte(&buf, 'I');
			pq_sendint32(&buf, collected_relation->key.relid);
			pq_sendbyte(&buf, collected_relation->region);
			pq_sendint32(&buf, collected_relation->nitems);
			items = &collected_relation->pages;
		}
		else
		{
			pq_sendbyte(&buf, 'T');
			pq_sendint32(&buf, collected_relation->key.relid);
			pq_sendbyte(&buf, collected_relation->region);
			pq_sendint32(&buf, collected_relation->nitems);
			items = &collected_relation->tuples;
		}

		pq_sendbytes(&buf, items->data, items->len);

		read_len += buf.len;
	}

	/* Update the length of the read section */
	*(int *) (buf.data + buf.cursor) = pg_hton32(read_len);

	pq_sendbytes(&buf, rwset_collection_buffer->writes.data, rwset_collection_buffer->writes.len);

	/* Actually send the buffer to the xact server */
	if (PQputCopyData(XactServerConn, buf.data, buf.len) <= 0 || PQflush(XactServerConn))
		ereport(WARNING, errmsg("[remotexact] failed to send read/write set"));

	/*
	 * TODO(ctring): This code is for debugging rwset remove all after the
	 * remote worker is implemented
	 */
	rwset = RWSetAllocate();
	buf.cursor = 0;
	RWSetDecode(rwset, &buf);
	ereport(LOG, errmsg("[remotexact] sent: %s", RWSetToString(rwset)));
	RWSetFree(rwset);
}

static CollectedRelation *
get_collected_relation(Oid relid, bool create_if_not_found)
{
	CollectedRelationKey key;
	CollectedRelation *relation;
	bool		found;

	Assert(rwset_collection_buffer);

	key.relid = relid;

	/* Check if the relation is in the map */
	relation = (CollectedRelation *) hash_search(rwset_collection_buffer->collected_relations,
												 &key, HASH_ENTER, &found);
	/* Initialize a new relation entry if not found */
	if (!found) {
		if (create_if_not_found)
		{
			MemoryContext old_context;

			old_context = MemoryContextSwitchTo(rwset_collection_buffer->context);

			relation->nitems = 0;
			relation->region = UNKNOWN_REGION;
			relation->is_index = false;
			initStringInfo(&relation->pages);
			initStringInfo(&relation->tuples);

			MemoryContextSwitchTo(old_context);
		}
		else
			relation = NULL;
	}
	return relation;
}

static bool
connect_to_txn_server(void)
{
	PGresult   *res;

	/* Reconnect if the connection is bad for some reason */
	if (Connected && PQstatus(XactServerConn) == CONNECTION_BAD)
	{
		PQfinish(XactServerConn);
		XactServerConn = NULL;
		Connected = false;

		ereport(LOG, errmsg("[remotexact] connection to transaction server broken, reconnecting..."));
	}

	if (Connected)
	{
		ereport(LOG, errmsg("[remotexact] reuse existing connection to transaction server"));
		return true;
	}

	XactServerConn = PQconnectdb(remotexact_connstring);

	if (PQstatus(XactServerConn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(XactServerConn));

		PQfinish(XactServerConn);
		ereport(WARNING,
				errmsg("[remotexact] could not connect to the transaction server"),
				errdetail_internal("%s", msg));
		return Connected;
	}

	/* TODO(ctring): send a more useful starting message */
	res = PQexec(XactServerConn, "start");
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ereport(WARNING, errmsg("[remotexact] invalid response from transaction server"));
		return Connected;
	}
	PQclear(res);

	Connected = true;

	ereport(LOG, errmsg("[remotexact] connected to transaction server"));

	return Connected;
}

static const RemoteXactHook remote_xact_hook =
{
	.collect_region = rx_collect_region,
	.collect_tuple = rx_collect_tuple,
	.collect_relation = rx_collect_relation,
	.collect_page = rx_collect_page,
	.clear_rwset = rx_clear_rwset_collection_buffer,
	.collect_insert = rx_collect_insert,
	.collect_update = rx_collect_update,
	.collect_delete = rx_collect_delete,
	.send_rwset_and_wait = rx_send_rwset_and_wait
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("remotexact.connstring",
							   "connection string to the transaction server",
							   NULL,
							   &remotexact_connstring,
							   "postgresql://127.0.0.1:10000",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	if (remotexact_connstring && remotexact_connstring[0])
	{
		SetRemoteXactHook(&remote_xact_hook);

		ereport(LOG, errmsg("[remotexact] initialized"));
		ereport(LOG, errmsg("[remotexact] xactserver connection string \"%s\"", remotexact_connstring));
	}
}
