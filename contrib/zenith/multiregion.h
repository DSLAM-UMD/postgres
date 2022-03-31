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

void DefineMultiRegionCustomVariables(void);

void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected);
void set_region_lsn(int region, ZenithResponse *msg);
XLogRecPtr get_region_lsn(int region);
void clear_region_lsns(void);

#endif