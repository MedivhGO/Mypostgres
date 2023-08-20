// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "assert.h"
#include "common.hpp"

#include <sys/stat.h>
#include <unistd.h>

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "access/relation.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif

}
// clang-format on

struct Db721FdwPlanState
{
  char *filename;
  List *attrs_sorted;
  bool use_mmap;
  bool use_thread;
  int32 max_open_files;
  bool files_in_orders;
  List *column_list;
  BlockNumber pages;
  double ntuples;
};

static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  Db721FdwPlanState *fdw_private) {
    struct stat stat_buf;
    BlockNumber pages;
    double ntuples;
    double nrows;

    if (stat(fdw_private->filename, &stat_buf) < 0) {
      stat_buf.st_size = 10 * BLCKSZ;
    }
    baserel->rows = nrows;
}

static void
get_table_options(Oid relid, Db721FdwPlanState *fdw_private)
{
  ForeignTable *table = GetForeignTable(relid);
  ForeignServer *server = GetForeignServer(table->serverid);
  ForeignDataWrapper *wrapper = GetForeignDataWrapper(server->fdwid);

  char *funcname = NULL;
  char *funcarg = NULL;
  fdw_private->use_mmap = false;
  fdw_private->use_thread = false;
  fdw_private->max_open_files = 0;
  fdw_private->files_in_orders = false;
}

/**
 * GetForeignRelSize should update the baserel rows and potentially also width. This will later be used by the optimizer.
 * If not the correct values are set it could lead to potential miss optimization.
 * This is also the place where we will handle the FDW options.
 * In order to send information to the next step of the planing we will store the
 * information inside ForeignScan node using the void *fdw_private that is provided by postgres.
 * fdw_private will not be touched by anything else and is it is free to store anything of interest within it.
 */

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid)
{
  elog(LOG, "begin db721_GetForeignRelSize foreigntable id is : %d", foreigntableid);
  Db721FdwPlanState *fdw_private;
  RangeTblEntry *rte;
  Relation rel;
  TupleDesc tupleDesc;
  uint64 matched_rows = 0;
  uint64 total_rows = 0;
  fdw_private = (Db721FdwPlanState *)palloc0(sizeof(Db721FdwPlanState));
  get_table_options(foreigntableid, fdw_private);
  rte = root->simple_rte_array[baserel->relid];
  rel = table_open(rte->relid, AccessShareLock);
  tupleDesc = RelationGetDescr(rel);
  baserel->fdw_private = fdw_private;
  baserel->tuples = total_rows;
  estimate_size(root,baserel, fdw_private);
  elog(LOG, "db721_GetForeignRelSize  %d", 10);
}

/**
 * GetForeignPaths describes the paths to access the data.
 * In our case there will only be one. Each paths should include a cost estimate.
 * This will be used by the optimizer to find the optimal path. This is set on the baserel->pathlist.
 */

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid)
{
  // TODO(721): Write me!
  Dog scout("Scout");
  elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());
}

/**
 * GetForeignPlan ir responsible for creating a ForeignScan * for the given ForeignPath *.
 * As input the optimizer has selected the best access path(in our case there will only be one).
 * Here we will also be able to pass information on to the next group of steps of the processing,
 * [Begin, Iterate, End](# Begin, Iterate, End) where we will execute the plan, using the void *fdw_state.
 * However void *fdw_state is a list so if the information from void *fdw_private should be propagate id needs to be reformated.
 */

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                     ForeignPath *best_path, List *tlist, List *scan_clauses,
                     Plan *outer_plan)
{
  Index scan_relid = baserel->relid;
  // TODO(721): Write me!
  return make_foreignscan(tlist,
                          scan_clauses,
                          scan_relid,
                          NIL,
                          best_path->fdw_private,
                          NIL,
                          NIL,
                          outer_plan);
}

/**
 * BeginForeignScan should do any initialization that is needed before the scan.
 * Information from the planing state can be accessed through ForeignScanState
 * and the underlying ForeignScan which contains fdw_private which is provided through
 * the previous planing and specifically GetForeignPlan.
 * To pass information further the fdw_state on the ForeignScanState can be used.
 */

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags)
{
  // TODO(721): Write me!
}

/**
 * IterateForeignScan should fetch one row(only),
 * if all data is returned NULL should be returned marking the end. ScanTupleSlot should be used for the data return.
 * Either a physical or virtual tuple should be returned. The rows returned must match the table definition of the FDW table.
 */

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node)
{
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node)
{
  // TODO(721): Write me!
}

/**
 * EndForeignScan end the scan and release resources. It is normally not important to release palloc’d memory,
 * but for example open files and connections to remote servers should be cleaned up.
 */

extern "C" void db721_EndForeignScan(ForeignScanState *node)
{
  // TODO(721): Write me!
}