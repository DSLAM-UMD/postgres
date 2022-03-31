/*-------------------------------------------------------------------------
 *
 * multiregion.c
 *	  Handles network communications in a multi-region setup.
 *
 * IDENTIFICATION
 *	 contrib/zenith/multiregion.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "multiregion.h"
#include "replication/walproposer.h"

#include "access/remotexact.h"
#include "catalog/catalog.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#define ZENITH_TAG "[ZENITH_SMGR] "
#define zenith_log(tag, fmt, ...) ereport(tag,		\
		(errmsg(ZENITH_TAG fmt, ##__VA_ARGS__), 	\
		errhidestmt(true), errhidecontext(true)))

/* GUCs */
static char *zenith_region_timelines;
static char *zenith_region_safekeeper_addrs;

static XLogRecPtr	*region_lsns = NULL;
static int	num_regions = 1;

static bool
check_zenith_region_timelines(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *timelines;
	ListCell *l;
	uint8 zid[16];

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of timeline ids */
	if (!SplitIdentifierString(rawstring, ',', &timelines))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(timelines);
		return false;
	}

	/* Check if the given timeline ids are valid */
	foreach (l, timelines)
	{
		char *tok = (char *)lfirst(l);

		/* Taken from check_zenith_id in libpagestore.c */
		if (*tok != '\0' && !HexDecodeString(zid, tok, 16))
		{
			GUC_check_errdetail("Invalid Zenith id: \"%s\".", tok);
			pfree(rawstring);
			list_free(timelines);
			return false;
		}
	}

	*extra = malloc(sizeof(int));
	if (!*extra)
		return false;

	*((int *) *extra) = list_length(timelines);

	pfree(rawstring);
	list_free(timelines);
	return true;
}

static void assign_zenith_region_timelines(const char *newval, void *extra)
{
	/* Add 1 for the global region */
	num_regions = *((int *) extra) + 1;
}

static bool
check_zenith_region_safekeeper_addrs(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *safekeeper_addrs;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of safekeeper_addrs */
	if (!SplitIdentifierString(rawstring, ',', &safekeeper_addrs))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(safekeeper_addrs);
		return false;
	}

	pfree(rawstring);
	list_free(safekeeper_addrs);

	return true;
}

void
DefineMultiRegionCustomVariables(void)
{
	DefineCustomStringVariable("zenith.region_timelines",
								"List of timelineids corresponding to the partitions. The first timeline is always for the global partition.",
								NULL,
								&zenith_region_timelines,
								"",
								PGC_POSTMASTER,
								GUC_LIST_INPUT, /* no flags required */
								check_zenith_region_timelines, assign_zenith_region_timelines, NULL);

	DefineCustomStringVariable("zenith.region_safekeeper_addrs",
								"List of addresses to the safekeepers in every regions. The first address is always for the global partition",
								NULL,
								&zenith_region_safekeeper_addrs,
								"",
								PGC_POSTMASTER,
								GUC_LIST_INPUT, /* no flags required */
								check_zenith_region_safekeeper_addrs, NULL, NULL);
}

static bool
split_into_host_and_port(const char *addr, char** host, char** port)
{
	int 		hostlen;
	int 		portlen;
	const char 	*colon = strchr(addr, ':');

	if (colon == NULL)
		return false;

	hostlen = colon - addr;
	portlen = strlen(addr) - hostlen - 1;

	*host = (char *)palloc(hostlen + 1);
	*port = (char *)palloc(portlen + 1);
	if (*host == NULL || *port == NULL)
		return false;

	strncpy(*host, addr, hostlen);
	(*host)[hostlen] = '\0';
	strncpy(*port, colon + 1, portlen);
	(*port)[portlen] = '\0';

	return true;
}

/*
 * Similar function to zenith_connect in libpagestore.c but used for
 * multiple timelines.
 */
void
zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected)
{
	List *timelines;
	ListCell *lc_timeline;
	List *safekeeper_addrs;
	ListCell *lc_addr;
	char *query;
	int ret;

	Assert(!*connected);

	*pageserver_conn = PQconnectdb(page_server_connstring);

	if (PQstatus(*pageserver_conn) == CONNECTION_BAD)
	{
		char *msg = pchomp(PQerrorMessage(*pageserver_conn));

		PQfinish(*pageserver_conn);
		*pageserver_conn = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				errmsg("[ZENITH_SMGR] could not establish connection"),
				errdetail_internal("%s", msg)));
	}

	/* These strings are already syntax-checked by GUC hooks */
	Assert(SplitIdentifierString(pstrdup(zenith_region_timelines), ',', &timelines));
	Assert(SplitIdentifierString(pstrdup(zenith_region_safekeeper_addrs), ',', &safekeeper_addrs));

	/* Make sure that they are the same length */
	Assert(list_length(timelines) == list_length(safekeeper_addrs));

	/* 
	 * Ask the Page Server to connect to other regions
	 */
	forboth (lc_timeline, timelines, lc_addr, safekeeper_addrs)
	{
		PGresult *res;
		int i = foreach_current_index(lc_timeline);
		char *timeline = (char *)lfirst(lc_timeline);
		char *addr = (char *)lfirst(lc_addr);
		char *host, *port;

		/* Skip the global partition and current partition */
		if (i == 0 || i == current_region)
			continue;
		
		if (!split_into_host_and_port(addr, &host, &port))
		{
			PQfinish(*pageserver_conn);
			*pageserver_conn = NULL;
			zenith_log(ERROR, "[ZENITH_SMGR] invalid safekeeper address \"%s\"", addr);
		}

		/* Connection string format taken from callmemaybe.rs in walkeeper */
		query = psprintf(
			"callmemaybe %s %s host=%s port=%s options='-c ztenantid=%s ztimelineid=%s pageserver_connstr=%s'",
			zenith_tenant, timeline, host, port, zenith_tenant, timeline, page_server_connstring);

		pfree(host);
		pfree(port);

		res = PQexec(*pageserver_conn, query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQfinish(*pageserver_conn);
			*pageserver_conn = NULL;
			zenith_log(ERROR, "[ZENITH_SMGR] callmemaybe command failed for timeline %s", timeline);
		}
		PQclear(res);
	}

	list_free(timelines);
	list_free(safekeeper_addrs);

	query = psprintf("multipagestream %s %s", zenith_tenant, zenith_region_timelines);
	ret = PQsendQuery(*pageserver_conn, query);
	if (ret != 1)
	{
		PQfinish(*pageserver_conn);
		*pageserver_conn = NULL;
		zenith_log(ERROR,
				   "[ZENITH_SMGR] failed to start dispatcher_loop on pageserver");
	}

	while (PQisBusy(*pageserver_conn))
	{
		int wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   	WL_LATCH_SET | WL_SOCKET_READABLE |
								WL_EXIT_ON_PM_DEATH,
								PQsocket(*pageserver_conn),
								-1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(*pageserver_conn))
			{
				char *msg = pchomp(PQerrorMessage(*pageserver_conn));

				PQfinish(*pageserver_conn);
				*pageserver_conn = NULL;

				zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
									 msg);
			}
		}
	}

	// FIXME: when auth is enabled this ptints JWT to logs
	zenith_log(LOG, "libpqpagestore: connected to '%s'", page_server_connstring);

	*connected = true;
}

static void
init_region_lsns()
{
	region_lsns = (XLogRecPtr *)
			MemoryContextAllocZero(TopTransactionContext,
								   num_regions * sizeof(XLogRecPtr));
}

/*
 * Set the LSN for a given region if it wasn't previously set. The set LSN is use
 * for that region throughout the life of the transaction.
 */
void
set_region_lsn(int region, ZenithResponse *msg)
{
	XLogRecPtr lsn;

	if (!IsMultiRegion() || !RegionIsRemote(region))
		return;

	AssertArg(region < num_regions);

	switch (messageTag(msg))
	{
		case T_ZenithExistsResponse:
			lsn = ((ZenithExistsResponse *) msg)->lsn;
			break;
		case T_ZenithNblocksResponse:
			lsn = ((ZenithNblocksResponse *) msg)->lsn;
			break;
		case T_ZenithGetPageResponse:
			lsn = ((ZenithGetPageResponse *) msg)->lsn;
			break;
		case T_ZenithGetSlruPageResponse:
			lsn = ((ZenithGetSlruPageResponse *) msg)->lsn;
			break;
		case T_ZenithErrorResponse:
			break;
		default:
			zenith_log(ERROR, "unexpected zenith message tag 0x%02x", messageTag(msg));
			break;
	}

	Assert(lsn != InvalidXLogRecPtr);

	if (region_lsns == NULL)
		init_region_lsns();

	if (region_lsns[region] == InvalidXLogRecPtr)
		region_lsns[region] = lsn;
	else
		Assert(region_lsns[region] == lsn);
}

/*
 * Get the LSN of a region
 */
XLogRecPtr
get_region_lsn(int region)
{
	if (!IsMultiRegion())
		return InvalidXLogRecPtr;
	
	// LSN of the current region is already tracked by postgres
	AssertArg(region != current_region);
	AssertArg(region < num_regions);

	if (region_lsns == NULL)
		init_region_lsns();

	return region_lsns[region];
}

void
clear_region_lsns(void)
{
	/* The data is destroyed along with the transaction
		context so only need to set this to NULL */
	region_lsns = NULL;
}