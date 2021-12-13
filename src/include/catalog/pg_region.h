/*-------------------------------------------------------------------------
 *
 * pg_region.h
 *	  definition of the "region" system catalog (pg_region)
 *
 * src/include/catalog/pg_region.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_REGION
#define PG_REGION

#include "catalog/genbki.h"

/* ----------------
 *		pg_region definition.  cpp turns this into
 *		typedef struct FormData_pg_region
 * ----------------
 */
CATALOG(pg_region,8000,RegionRelationId)
{
    /* Id of the region */
	int32		regid;
    /* Name of the region */
	NameData	regname;
} FormData_pg_region;

/* ----------------
 *		Form_pg_region corresponds to a pointer to a tuple with
 *		the format of pg_region relation.
 * ----------------
 */
typedef FormData_pg_region *Form_pg_region;

DECLARE_UNIQUE_INDEX(pg_region_regname_index, 8001, on pg_region using btree(regname name_ops));
#define RegionRegnameIndexId 8001
DECLARE_UNIQUE_INDEX_PKEY(pg_region_regid_index, 8002, on pg_region using btree(regid int4_ops));
#define RegionRegidIndexId  8002


#endif							/* PG_REGION */
