/*-------------------------------------------------------------------------
 *
 * pg_locality.h
 *	  definition of the "locality" system catalog (pg_locality)
 *
 * src/include/catalog/pg_locality.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOCALITY
#define PG_LOCALITY

#include "catalog/genbki.h"

/* ----------------
 *		pg_locality definition.  cpp turns this into
 *		typedef struct FormData_pg_locality
 * ----------------
 */
CATALOG(pg_locality,8003,LocalityRelationId)
{
    /* Oid of the relation */
	Oid			relid BKI_LOOKUP(pg_class);
    /* Id of the owning region */
    int32       regid;
} FormData_pg_locality;

/* ----------------
 *		Form_pg_locality corresponds to a pointer to a tuple with
 *		the format of pg_locality relation.
 * ----------------
 */
typedef FormData_pg_locality *Form_pg_locality;

DECLARE_UNIQUE_INDEX_PKEY(pg_locality_relid_index, 8004, on pg_locality using btree(relid oid_ops));
#define LocalityRelidIndexId  8004

DECLARE_FOREIGN_KEY((regid), pg_region, (regid));

#endif							/* PG_LOCALITY */
