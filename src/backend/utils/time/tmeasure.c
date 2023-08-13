#include "postgres.h"

#include "access/xact.h"
#include "portability/instr_time.h"
#include "utils/tmeasure.h"

#define MAX_MEASURE 5000
#define MAX_NAME_LEN 64

typedef struct MeasureData
{
  char          name[MAX_NAME_LEN];
  instr_time	duration;
} MeasureData;

static MeasureData measure_data[MAX_MEASURE];
static int measure_data_len = 0;

static bool measure_started = false;
static instr_time measure_start;


void start_time_measure(const char *name) {
    Assert(!measure_started);
    Assert(measure_data_len < MAX_MEASURE);

    if (name != NULL)
        strncpy(measure_data[measure_data_len].name, name, MAX_NAME_LEN);

    measure_started = true;
    INSTR_TIME_SET_CURRENT(measure_start);
}

void finish_time_measure(const char *name) {
    Assert(measure_started);

    if (name != NULL)
        strncpy(measure_data[measure_data_len].name, name, MAX_NAME_LEN);

    INSTR_TIME_SET_CURRENT(measure_data[measure_data_len].duration);
    INSTR_TIME_SUBTRACT(measure_data[measure_data_len].duration, measure_start);

    measure_data_len++;
    measure_started = false;
}

void log_time_measure(void) {
	StringInfoData s;
    int i;
    // Use this command id so that we also count the read-only ones
    CommandId cid = GetCurrentCommandId(true);

    Assert(!measure_started);

	initStringInfo(&s);
    for (i = 0; i < measure_data_len; i++) {
        if (i > 0)
            appendStringInfo(&s, ", ");

        appendStringInfo(&s, "[\"%s\", %f]", measure_data[i].name, INSTR_TIME_GET_MILLISEC(measure_data[i].duration));
    }

    elog(LOG, "CMD %d [%s]", cid, s.data);

    measure_data_len = 0;
}