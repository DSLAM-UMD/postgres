/*-------------------------------------------------------------------------
 *
 * remotexact.h
 *
 * src/include/access/remotexact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTEXACT_H
#define REMOTEXACT_H

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "utils/relcache.h"
#include "storage/itemptr.h"

#define UNKNOWN_REGION -1
#define GLOBAL_REGION 0
#define MAX_REGIONS 64 // 0 reserved for GLOBAL_REGION and 1..63 for user regions.

#define IsMultiRegion() (current_region != GLOBAL_REGION)
#define RegionIsValid(r) (r != UNKNOWN_REGION)
#define RegionIsRemote(r) (RegionIsValid(r) && r != current_region && r != GLOBAL_REGION)

/*
 * RelationGetRegion
 *		Fetch relation's region.
 */
#define RelationGetRegion(relation) ((relation)->rd_rel->relregion)

/*
 * RelationIsRemote
 *		Determine if a relation is a remote relation
 */
#define RelationIsRemote(relation) (RegionIsRemote(RelationGetRegion(relation)))

/* GUC variable */
extern int current_region;

typedef XLogRecPtr (*get_region_lsn_hook_type) (int region);
extern PGDLLIMPORT get_region_lsn_hook_type get_region_lsn_hook;

#define GetRegionLsn(r) (get_region_lsn_hook == NULL ? InvalidXLogRecPtr : (*get_region_lsn_hook)(r))

typedef struct
{
	void		(*collect_relation) (int region, Oid dbid, Oid relid);
	void		(*collect_page) (int region, Oid dbid, Oid relid, BlockNumber blkno);
	void		(*collect_tuple) (int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset);
	void		(*collect_insert) (Relation relation, HeapTuple newtuple);
	void		(*collect_update) (Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
	void		(*collect_delete) (Relation relation, HeapTuple oldtuple);
	void		(*execute_remote_xact) (void);
} RemoteXactHook;

extern void SetRemoteXactHook(const RemoteXactHook *hook);

extern void CollectRelation(int region, Oid dbid, Oid relid);
extern void CollectPage(int region, Oid dbid, Oid relid, BlockNumber blkno);
extern void CollectTuple(int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset);
extern void CollectInsert(Relation relation, HeapTuple newtuple);
extern void CollectUpdate(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
extern void CollectDelete(Relation relation, HeapTuple oldtuple);

extern bool is_surrogate;
extern void PreCommit_ExecuteRemoteXact(void);

#endif							/* REMOTEXACT_H */
