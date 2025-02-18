// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "assert.h"
#include "common.h"
#include "myjson.h"
#include "myfilereader.h"
#include "myexecstat.h"

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
#include "nodes/pg_list.h"
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
#include "utils/json.h"
#include "executor/executor.h"
#include "nodes/print.h"
#include "catalog/pg_type.h"
#include "utils/fmgrprotos.h"

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
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
               Db721FdwPlanState *fdw_private,
               Cost *startup_cost, Cost *total_cost)
{
  *startup_cost = baserel->baserestrictcost.startup;
  *total_cost = *startup_cost;
  baserel->rows = 100;
}

static void 
print_op_expr(OpExpr *opexpr)
{
    /* 获取运算符的名称和左右操作数 */
    Oid operatorId = opexpr->opno;
    Oid leftTypeId, rightTypeId;
    char *operatorName = get_opname(operatorId);
    Node *leftArg, *rightArg;
    leftArg = (Node *)linitial(opexpr->args);
    rightArg = (Node *)lsecond(opexpr->args);

    /* 获取左右操作数的类型 */
    leftTypeId = exprType(leftArg);
    rightTypeId = exprType(rightArg);

    /* 使用 elog 函数将 OpExpr 的内容打印出来 */
    elog(LOG, "OpExpr: %s (%u), leftArg: %s (%u), rightArg: %s (%u)",
         operatorName, operatorId,
         format_type_be(leftTypeId), leftTypeId,
         format_type_be(rightTypeId), rightTypeId);
}

static void
set_filter(List *scan_clauses, Db721Filter &filter)
{
  ListCell *lc;

  foreach (lc, scan_clauses)
  {
    elog(LOG, "where cluase iterate one");
    Expr *clause = (Expr *)lfirst(lc);
    OpExpr *expr;
    Expr *left, *right;
    Const *c;
    Var *v;
    Oid opno = InvalidOid;

    if (IsA(clause, RestrictInfo))
      clause = ((RestrictInfo *)clause)->clause;

    if (IsA(clause, OpExpr))
    {
      expr = (OpExpr *)clause;
      print_op_expr(expr);

      /* Only interested in binary opexprs */
      if (list_length(expr->args) != 2)
        continue;

      left = (Expr *)linitial(expr->args);
      right = (Expr *)lsecond(expr->args);

      /*
       * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
       *
       * XXX Currently only Var as expression is supported. Will be
       * extended in future.
       */
      if (IsA(right, Const))
      {
        if (!IsA(left, Var))
          continue;
        v = (Var *)left;
        c = (Const *)right;
        opno = expr->opno;
      }
      else if (IsA(left, Const))
      {
        /* reverse order (CONST OP VAR) */
        if (!IsA(right, Var))
          continue;
        v = (Var *)right;
        c = (Const *)left;
        opno = get_commutator(expr->opno);
      }
      else
      {
        continue;
      }

      filter.attnum = v->varattno;
      filter.value = c;
    }
  }
}

static void
destory_db721_state(void *arg)
{
  Db721FdwExecutionState *festate = (Db721FdwExecutionState *)arg;
  if (festate)
  {
    delete festate;
  }
}

/**
 * parser db721 file
 */
static void
parser_db721_file(std::string_view json, Db721FdwPlanState *fdw_private) noexcept
{
  auto [obj, eaten] = myutil::parse(json);
  myutil::JSONDict mymeta = obj.get<myutil::JSONDict>();
  fdw_private->max_values_per_block = mymeta["Max Values Per Block"]->get<int>();
  myutil::JSONDict my_columns = mymeta["Columns"]->get<myutil::JSONDict>();
  for (std::pair<std::string, std::shared_ptr<myutil::JSONObject>> one_column : my_columns)
  {
    fdw_private->columns_list.push_back(one_column.first);
    myutil::JSONDict column_desc = one_column.second->get<myutil::JSONDict>();
    ColumnDesc cd;
    cd.colum_name = one_column.first;
    cd.type_name = column_desc["type"]->get<std::string>();
    cd.start_offset = column_desc["start_offset"]->get<int>();
    cd.num_blocks = column_desc["num_blocks"]->get<int>();
    myutil::JSONDict myblockstat = column_desc["block_stats"]->get<myutil::JSONDict>();
    for (std::pair<std::string, std::shared_ptr<myutil::JSONObject>> item : myblockstat)
    {
      if (cd.type_name == "str")
      {
        StringColumnBlockStat scbt;
        myutil::JSONDict cur_stat = item.second->get<myutil::JSONDict>();
        scbt.max = cur_stat["max"]->get<std::string>();
        scbt.min = cur_stat["min"]->get<std::string>();
        scbt.str_max_len = cur_stat["max_len"]->get<int>();
        scbt.str_min_len = cur_stat["min_len"]->get<int>();
        scbt.value_in_block = cur_stat["num"]->get<int>();
        cd.str_block_stat.insert(make_pair(item.first, scbt));
      }
      else if (cd.type_name == "float")
      {
        FloatColumnBlockStat fcbs;
        myutil::JSONDict cur_stat = item.second->get<myutil::JSONDict>();
        fcbs.value_in_block = cur_stat["num"]->get<int>();
        // may be the value be recongnized int
        // so need to judge
        fcbs.max = cur_stat["max"]->is<int>() ? cur_stat["max"]->get<int>() : cur_stat["max"]->get<float>();
        fcbs.min = cur_stat["min"]->is<int>() ? cur_stat["min"]->get<int>() : cur_stat["min"]->get<float>();
        cd.float_block_stat.insert(make_pair(item.first, fcbs));
      }
      else if (cd.type_name == "int")
      {
        IntColumnBlockStat icbs;
        myutil::JSONDict cur_stat = item.second->get<myutil::JSONDict>();
        icbs.max = cur_stat["max"]->get<int>();
        icbs.min = cur_stat["min"]->get<int>();
        icbs.value_in_block = cur_stat["num"]->get<int>();
        cd.int_block_stat.insert(make_pair(item.first, icbs));
      }
    }
    fdw_private->columns_desc.push_back(cd);
  }
}

static void
get_table_options(Oid relid, Db721FdwPlanState *fdw_private)
{
  ForeignTable *table = GetForeignTable(relid);
  ListCell *lc;
  foreach (lc, table->options)
  {
    DefElem *def = (DefElem *)lfirst(lc);
    if (strcmp(def->defname, "filename") == 0)
    {
      fdw_private->filename = defGetString(def);
    }
    else if (strcmp(def->defname, "tablename") == 0)
    {
      fdw_private->tablename = defGetString(def);
    }
    else
    {
      elog(ERROR, "unknown option '%s'", def->defname);
    }
  }
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
  Db721FdwPlanState *fdw_private = (Db721FdwPlanState *)palloc0(sizeof(Db721FdwPlanState));
  uint64 total_rows = 0;
  Db721Filter filter;
  get_table_options(foreigntableid, fdw_private);
  set_filter(baserel->baserestrictinfo, filter);
  myutil::FileReader openfile;
  openfile.Open(fdw_private->filename);
  uint32_t meta_size = openfile.Seek(-JSON_META_SIZE, std::ios_base::end).ReadUInt32();
  size_t joson_begin = JSON_META_SIZE + meta_size;
  std::string meta_json = openfile.Seek(-joson_begin, std::ios_base::end).ReadAsciiString(meta_size);
  elog(LOG, "%s", meta_json.c_str());
  openfile.Close();
  parser_db721_file(meta_json, fdw_private);
  baserel->fdw_private = fdw_private;
  baserel->tuples = total_rows;
}

/**
 * GetForeignPaths describes the paths to access the data.
 * In our case there will only be one. Each paths should include a cost estimate.
 * This will be used by the optimizer to find the optimal path. This is set on the baserel->pathlist.
 */

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid)
{
  Db721FdwPlanState *fdw_private = (Db721FdwPlanState *)baserel->fdw_private;
  Path *foreign_path;
  Cost startup_cost;
  Cost total_cost;

  estimate_costs(root, baserel, fdw_private,
                 &startup_cost, &total_cost);

  foreign_path = (Path *)create_foreignscan_path(root, baserel,
                                                 NULL, /* default pathtarget */
                                                 baserel->rows,
                                                 startup_cost,
                                                 total_cost,
                                                 NULL, /* no pathkeys */
                                                 NULL, /* no outer rel either */
                                                 NULL, /* no extra plan */
                                                 (List *)fdw_private);
  add_path(baserel, (Path *)foreign_path);
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
  scan_clauses = extract_actual_clauses(scan_clauses, false);
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
  MemoryContextCallback *callback;
  ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
  EState *estate = node->ss.ps.state;
  MemoryContext reader_cxt;
  MemoryContext cxt = estate->es_query_cxt;
  reader_cxt = AllocSetContextCreate(cxt, "db721_fdw tuple data", ALLOCSET_DEFAULT_SIZES);
  Db721FdwPlanState *fdw_private = (Db721FdwPlanState *)plan->fdw_private;
  Db721FdwExecutionState *festate = new Db721FdwExecutionState(fdw_private->filename, fdw_private->columns_desc, reader_cxt);
  festate->open();

  callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
  callback->func = destory_db721_state;
  callback->arg = (void *)festate;
  MemoryContextRegisterResetCallback(reader_cxt, callback);
  node->fdw_state = festate;
}

/**
 * IterateForeignScan should fetch one row(only),
 * if all data is returned NULL should be returned marking the end. ScanTupleSlot should be used for the data return.
 * Either a physical or virtual tuple should be returned. The rows returned must match the table definition of the FDW table.
 */

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node)
{
  Db721FdwExecutionState *festate = (Db721FdwExecutionState *)node->fdw_state;
  std::string error;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  ExecClearTuple(slot);
  try
  {
    festate->next(slot);
  }
  catch (std::exception &e)
  {
    error = e.what();
  }
  if (!error.empty())
  {
    elog(ERROR, "db721_fdw: %s", error.c_str());
  }
  return slot;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node)
{
  Db721FdwExecutionState *festate = (Db721FdwExecutionState *)node->fdw_state;
  festate->rescan();
}

/**
 * EndForeignScan end the scan and release resources. It is normally not important to release palloc’d memory,
 * but for example open files and connections to remote servers should be cleaned up.
 */

extern "C" void db721_EndForeignScan(ForeignScanState *node)
{
}