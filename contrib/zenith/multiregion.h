/*-------------------------------------------------------------------------
 *
 * multiregion.h
 * 
 * contrib/zenith/multiregion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIREGION_H
#define MULTIREGION_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "libpq-fe.h"
#include "pagestore_client.h"

extern void DefineMultiRegionCustomVariables(void);

extern void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected);
extern void set_region_lsn(int region, ZenithResponse *msg);
extern XLogRecPtr get_region_lsn(int region);
extern void clear_region_lsns(void);

#endif