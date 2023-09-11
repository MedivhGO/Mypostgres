#include "common.h"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/timestamp.h"
#include "limits.h"
}

