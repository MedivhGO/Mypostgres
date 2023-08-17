// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "assert.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
}
// clang-format on

struct Db721FdwPlanState
{
    List * filename;
    List * attrs_sorted;
    bool   use_mmap;
    bool   use_thread;
    int32  max_open_files;
    bool   files_in_orders;
};

static void
get_table_options(Oid relid, Db721FdwPlanState* fdw_private)
{
    ForeignTable *table;
    char *funcname = NULL;
    char *funcarg = NULL;
    fdw_private->use_mmap = false;
    fdw_private->use_thread = false;
    fdw_private->max_open_files = 0;
    fdw_private->files_in_orders = false;
    table = GetForeignTable(relid);
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  Db721FdwPlanState *fdw_private;
  RangeTblEntry *rte;
  Relation rel;
  TupleDesc tupleDesc;
  List* filename_orig;
  uint64  matched_rows = 0;
  uint64  total_rows = 0;
  fdw_private = (Db721FdwPlanState *) palloc0(sizeof(Db721FdwPlanState));
  get_table_options(foreigntableid, fdw_private);
  rte = root->simple_rte_array[baserel->relid];
  rel = table_open(rte->relid, AccesShareLock);
  tupleDesc = RelationGetDescr(rel);

  filename_orig = fdw_private->filenames;
  baserel->fdw_private = fdw_private;
  baserel->tuples = total_rows;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  Dog scout("Scout");
  elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}