#ifndef DB721_FDW_COMMON_HPP
#define DB721_FDW_COMMON_HPP

#include <cstdarg>
#include <cstddef>
#include <iostream>
#include <exception>
#include <unordered_map>
#include <vector>
#include <variant>
#include <list>
#include <memory>
#include <algorithm>

#include "myfilereader.h"
#include "myallocator.h"

extern "C"
{
#include "../../../../src/include/postgres.h"
#include "utils/jsonb.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "access/htup_details.h"
#include "utils/rel.h"
};

#define ERROR_STR_LEN 512
#define JSON_META_SIZE 4

using FileReader = myutil::FileReader;
using FastAllocator = myutil::FastAllocator;

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
  uint32_t total_rows = 0;
  int cur_rows;
  std::string type_name;
};

struct Db721Filter
{
  AttrNumber attnum;
  Const* value;
};

class DB721FileReader : public FileReader
{
public:
  DB721FileReader(const std::string &file_path,
                  std::vector<ColumnDesc> col_desc,
                  MemoryContext cxt) : allocator_(new FastAllocator(cxt))
  {
    file_path_ = file_path;
    col_desc_ = col_desc;
  }

  ~DB721FileReader()
  {
    close();
  }

  void open()
  {
    Open(file_path_);
    init_column_reader();
    init_total_row();
  }

  void close()
  {
    if (HasOpen())
    {
      Close();
    }
  }

  bool next(TupleTableSlot *slot)
  {
    if (row_ > num_rows_)
    {
      return false;
    }
    fill_slot(slot);
    row_++;
    return true;
  }

  void fill_slot(TupleTableSlot *slot)
  {
    Form_pg_attribute attr_int;
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
    {
      attr_int = TupleDescAttr(slot->tts_tupleDescriptor, attr);
      std::string columnName(NameStr(attr_int->attname));
      slot->tts_values[attr] = read_at_icol(columnName);
    }
  }

  virtual auto ReadUInt8Array(size_t count) -> uint8_t * override
  {
    uint8_t *data = (uint8_t *)allocator_->fast_alloc(count);
    for (size_t x = 0; x < count; x++)
    {
      uint8_t u = ReadUInt8();
      data[x] = u;
    }
    return data;
  }

  void init_column_reader()
  {
    for (auto x : col_desc_)
    {
      ColumnReader cr;
      cr.start_offset = x.start_offset;
      cr.cur_rows = 0;
      if (x.type_name == "str")
      {
        cr.type_size = 32;
        cr.type_name = "str";
        for (auto item_block : x.str_block_stat)
        {
          cr.total_rows += item_block.second.value_in_block;
        }
      }
      else if (x.type_name == "float")
      {
        cr.type_size = 4;
        cr.type_name = "float";
        for (auto item_block : x.float_block_stat)
        {
          cr.total_rows += item_block.second.value_in_block;
        }
      }
      else if (x.type_name == "int")
      {
        cr.type_size = 4;
        cr.type_name = "int";
        for (auto item_block : x.int_block_stat)
        {
          cr.total_rows += item_block.second.value_in_block;
        }
      }
      col_reader_.insert(make_pair(x.colum_name, cr));
    }
  }

  void init_total_row()
  {
    std::pair<std::string, ColumnReader> item = *col_reader_.begin();
    num_rows_ = item.second.total_rows - 1;
  }

  void rescan()
  {
    row_ = 0;
  }

  Datum read_at_icol(std::string col_name)
  {
    Datum res = Int32GetDatum(-1);
    ColumnReader &cur_reader = col_reader_[col_name];
    std::string type_name = cur_reader.type_name;
    int cur_offset = cur_reader.start_offset + cur_reader.cur_rows * cur_reader.type_size;
    Seek(cur_offset, std::ios_base::beg);
    cur_reader.cur_rows++;
    if (type_name == "str")
    {
      uint8_t *data = ReadUInt8Array(32);
      int32_t len = strlen(reinterpret_cast<char *>(data));
      int64_t bytea_len = len + VARHDRSZ;
      bytea *b = (bytea *)allocator_->fast_alloc(bytea_len);
      SET_VARSIZE(b, bytea_len);
      memcpy(VARDATA(b), data, len);
      res = PointerGetDatum(b);
    }
    else if (type_name == "int")
    {
      void *four_data = allocator_->fast_alloc(4);
      Read4Bytes(reinterpret_cast<char *>(four_data));
      res = Int32GetDatum(*reinterpret_cast<int32_t *>(four_data));
    }
    else if (type_name == "float")
    {
      void *four_data = allocator_->fast_alloc(4);
      Read4Bytes(reinterpret_cast<char *>(four_data));
      res = Float4GetDatum(*reinterpret_cast<float4 *>(four_data));
    }
    return res;
  }

private:
  uint32_t row_ = 0;
  uint32_t num_rows_ = 0;
  std::vector<ColumnDesc> col_desc_;
  std::string file_path_;
  std::unordered_map<std::string, ColumnReader> col_reader_;
  std::unique_ptr<FastAllocator> allocator_;
};
#endif