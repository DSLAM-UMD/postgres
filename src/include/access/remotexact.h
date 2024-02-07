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

#define IsMultiRegion() (multi_region)
#define RegionIsValid(r) (r != UNKNOWN_REGION)
#define RegionIsRemote(r) (RegionIsValid(r) && r != current_region && multi_region)

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
extern bool multi_region;
extern int current_region;

typedef XLogRecPtr (*get_region_lsn_hook_type) (int region);
extern PGDLLIMPORT get_region_lsn_hook_type get_region_lsn_hook;
typedef XLogRecPtr *(*get_all_region_lsns_hook_type) (void);
extern PGDLLIMPORT get_all_region_lsns_hook_type get_all_region_lsns_hook;

#define GetRegionLsn(r) (get_region_lsn_hook == NULL ? InvalidXLogRecPtr : (*get_region_lsn_hook)(r))
#define GetAllRegionLsns() (get_all_region_lsns_hook == NULL ? NULL : (*get_all_region_lsns_hook)())

typedef enum MultiRegionXactState
{
	MULTI_REGION_XACT_NONE,		/* not a multi-region transaction */
	MULTI_REGION_XACT_STARTED,	/* accessed some remote data */
	MULTI_REGION_XACT_COMMITTING,	/* called prepare for the loca portion the multi-region transaction */
} MultiRegionXactState;

typedef struct
{
	void					(*collect_relation) (int region, Oid dbid, Oid relid, char relkind);
	void					(*collect_page) (int region, Oid dbid, Oid relid, BlockNumber blkno, char relkind);
	void					(*collect_tuple) (int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset, char relkind);
	void					(*collect_insert) (Relation relation, HeapTuple newtuple);
	void					(*collect_update) (Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
	void					(*collect_delete) (Relation relation, HeapTuple oldtuple);
	MultiRegionXactState	(*get_multi_region_xact_state) (void);
	void					(*prepare_multi_region_xact) (void);
	bool					(*commit_multi_region_xact) (void);
	void					(*report_multi_region_xact_error) (void);
} RemoteXactHook;

extern void SetRemoteXactHook(const RemoteXactHook *hook);

extern void CollectRelation(int region, Oid dbid, Oid relid, char relkind);
extern void CollectPage(int region, Oid dbid, Oid relid, BlockNumber blkno, char relkind);
extern void CollectTuple(int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset, char relkind);
extern void CollectInsert(Relation relation, HeapTuple newtuple);
extern void CollectUpdate(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
extern void CollectDelete(Relation relation, HeapTuple oldtuple);

extern MultiRegionXactState GetMultiRegionXactState(void);
extern void PrepareMultiRegionXact(void);
extern bool CommitMultiRegionXact(void);
extern void ReportMultiRegionXactError(void);

#endif							/* REMOTEXACT_H */
