#ifndef DB721_FDW_COMMON_HPP
#define DB721_FDW_COMMON_HPP

#include <cstdarg>
#include <cstddef>
#include <iostream>
#include <exception>
#include <unordered_map>
#include <vector>
#include <variant>

#include "myfilereader.h"

extern "C"
{
#include "../../../../src/include/postgres.h"
#include "utils/jsonb.h"
#include "executor/tuptable.h"
};

#define ERROR_STR_LEN 512
#define JSON_META_SIZE 4

using FileReader = myutil::FileReader;

template <typename T>
struct BlockStat
{
  int value_in_block;
  T min;
  T max;
  int str_max_len = 0;
  int str_min_len = 0;
};

using StringColumnBlockStat = BlockStat<std::string>;
using IntColumnBlockStat = BlockStat<int>;
using FloatColumnBlockStat = BlockStat<float>;

struct ColumnDesc
{
  std::string colum_name;
  std::string type_name;
  int num_blocks;
  int start_offset;
  std::unordered_map<std::string, StringColumnBlockStat> str_block_stat;
  std::unordered_map<std::string, IntColumnBlockStat> int_block_stat;
  std::unordered_map<std::string, FloatColumnBlockStat> float_block_stat;
};

typedef struct Db721FdwPlanState
{
  std::string filename;
  std::string tablename;
  int max_values_per_block;
  std::vector<std::string> columns_list;
  std::vector<ColumnDesc> columns_desc;
} Db721FdwPlanState;

struct ColumnReader
{
  int start_offset;
  int type_size;
  int total_rows;
  int cur_rows;
  std::string type_name;
};


class DB721FileReader : public FileReader
{
public:

  DB721FileReader(const std::string& file_path, std::vector<ColumnDesc> col_desc) {
    file_path_ = file_path;
    col_desc_ = col_desc;
  }

  ~DB721FileReader() {
    close();
  }

  void open() {
    Open(file_path_);
    init_column_reader();
  }

  void close() {
    if (HasOpen())
    {
      Close();
    }
  }

  bool next(TupleTableSlot* slot) {
    if (row_ > num_rows_ ) {
      return false;
    }
    fill_slot(slot);
    row_++;
    return true;
  }

  void fill_slot(TupleTableSlot* slot) {
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++) {
      if (attr == 0) {
        slot->tts_isnull[attr] = true;
        read_at_icol(attr);
      }
      slot->tts_values[attr] = read_at_icol(attr);
    }
  }

  void init_column_reader() {
    for (auto x : col_desc_) {
      ColumnReader cr;
      cr.start_offset = x.start_offset;
      cr.cur_rows = 0;
      if (x.type_name == "str") {
        cr.type_size = 32;
        cr.total_rows = 6;
        cr.type_name = "str";
      } else if (x.type_name == "float") {
        cr.type_size = 4;
        cr.total_rows = 6;
        cr.type_name = "float";
      } else if (x.type_name == "int") {
        cr.type_size = 4;
        cr.total_rows = 6;
        cr.type_name = "int";
      }
      col_reader_.push_back(cr);
    }
  }

  void rescan() {
    row_ = 0;
  }

  Datum read_at_icol(int it_col) {
    Datum res;
    ColumnReader& cur_reader = col_reader_[it_col];
    std::string type_name = cur_reader.type_name;
    int cur_offset = cur_reader.start_offset + cur_reader.cur_rows * cur_reader.type_size;
    Seek(cur_offset, std::ios_base::beg);
    cur_reader.cur_rows++;
    if (type_name == "str") {
      std::string values = ReadAsciiString(32);
      // char * ptr = (char*) palloc(32);
      // memcpy(ptr, values.c_str(), 32);
      // res = PointerGetDatum(ptr);
      elog(LOG, "%s", values.c_str());
    } else if (type_name == "int") {
      int32_t value;
      Read4Bytes(reinterpret_cast<char*>(&value));
      elog(LOG, "%d", value);
      res = Int32GetDatum(value);
    } else if (type_name == "float") {
      float value;
      Read4Bytes(reinterpret_cast<char*>(&value));
      elog(LOG, "%f", value);
      res = Float4GetDatum(value);
    } 
    return res;
  }

private:
  uint32_t row_ = 0;                    // cur_row
  uint32_t num_rows_ = 5;               // total_rows
  std::vector<ColumnDesc> col_desc_;
  std::string file_path_;               // file_path
  std::vector<ColumnReader> col_reader_;
};

class Db721FdwExecutionState
{
public:
  Db721FdwExecutionState(const std::string& file_path, std::vector<ColumnDesc> col_desc) :
    reader_(file_path, col_desc) {}

  ~Db721FdwExecutionState() {};

  bool next(TupleTableSlot *slot)
  {
    if (reader_.next(slot)) {
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
  DB721FileReader reader_;
};
#endif