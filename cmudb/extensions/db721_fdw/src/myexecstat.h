#pragma once

#include "common.h"

class Db721FdwExecutionState
{
public:
  Db721FdwExecutionState(const std::string &file_path,
                         std::vector<ColumnDesc> col_desc,
                         MemoryContext cxt) : cxt_(cxt), reader_(file_path, col_desc, cxt_) {}

  ~Db721FdwExecutionState(){};

  bool next(TupleTableSlot *slot)
  {
    if (reader_.next(slot))
    {
      ExecStoreVirtualTuple(slot);
    }
    return true;
  }

  void rescan(void)
  {
    reader_.rescan();
  }

  void open()
  {
    reader_.open();
  }

private:
  MemoryContext cxt_;
  DB721FileReader reader_;
};