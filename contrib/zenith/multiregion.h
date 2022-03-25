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

#include "libpq-fe.h"

void DefineMultiRegionCustomVariables(void);

bool zenith_multiregion_enabled(void);
void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected);

#endif