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

#include "myfilereader.h"

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
#define SEGMENT_SIZE (1024 * 1024)

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

void *
exc_palloc(std::size_t size)
{
	/* duplicates MemoryContextAllocZero to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	AssertArg(MemoryContextIsValid(context));

	if (!AllocSizeIsValid(size))
		throw std::bad_alloc();

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
		throw std::bad_alloc();

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}

class FastAllocator
{
private:
  /*
   * Special memory segment to speed up bytea/Text allocations.
   */
  MemoryContext segments_cxt;
  char *segment_start_ptr;
  char *segment_cur_ptr;
  char *segment_last_ptr;
  std::list<char *> garbage_segments;

public:
  FastAllocator(MemoryContext cxt)
      : segments_cxt(cxt), segment_start_ptr(nullptr), segment_cur_ptr(nullptr),
        segment_last_ptr(nullptr), garbage_segments()
  {
  }

  ~FastAllocator()
  {
    this->recycle();
  }

  /*
   * fast_alloc
   *      Preallocate a big memory segment and distribute blocks from it. When
   *      segment is exhausted it is added to garbage_segments list and freed
   *      on the next executor's iteration. If requested size is bigger that
   *      SEGMENT_SIZE then just palloc is used.
   */
  inline void *fast_alloc(long size)
  {
    void *ret;

    Assert(size >= 0);

    /* If allocation is bigger than segment then just palloc */
    if (size > SEGMENT_SIZE)
    {
      MemoryContext oldcxt = MemoryContextSwitchTo(this->segments_cxt);
      void *block = exc_palloc(size);
      this->garbage_segments.push_back((char *)block);
      MemoryContextSwitchTo(oldcxt);

      return block;
    }

    size = MAXALIGN(size);

    /* If there is not enough space in current segment create a new one */
    if (this->segment_last_ptr - this->segment_cur_ptr < size)
    {
      MemoryContext oldcxt;

      /*
       * Recycle the last segment at the next iteration (if there
       * was one)
       */
      if (this->segment_start_ptr)
        this->garbage_segments.push_back(this->segment_start_ptr);

      oldcxt = MemoryContextSwitchTo(this->segments_cxt);
      this->segment_start_ptr = (char *)exc_palloc(SEGMENT_SIZE);
      this->segment_cur_ptr = this->segment_start_ptr;
      this->segment_last_ptr =
          this->segment_start_ptr + SEGMENT_SIZE - 1;
      MemoryContextSwitchTo(oldcxt);
    }

    ret = (void *)this->segment_cur_ptr;
    this->segment_cur_ptr += size;

    return ret;
  }

  void recycle(void)
  {
    /* recycle old segments if any */
    if (!this->garbage_segments.empty())
    {
      bool error = false;

      PG_TRY();
      {
        for (auto it : this->garbage_segments)
          pfree(it);
      }
      PG_CATCH();
      {
        error = true;
      }
      PG_END_TRY();
      if (error)
        throw std::runtime_error("garbage segments recycle failed");

      this->garbage_segments.clear();
      elog(DEBUG1, "parquet_fdw: garbage segments recycled");
    }
  }

  MemoryContext context()
  {
    return segments_cxt;
  }
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

  virtual auto ReadUInt8Array(size_t count) -> uint8_t* override {
    uint8_t* data = (uint8_t*) allocator_->fast_alloc(count);
    for (size_t x = 0; x < count; x++) {
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
        cr.total_rows = 6;
        cr.type_name = "str";
      }
      else if (x.type_name == "float")
      {
        cr.type_size = 4;
        cr.total_rows = 6;
        cr.type_name = "float";
      }
      else if (x.type_name == "int")
      {
        cr.type_size = 4;
        cr.total_rows = 6;
        cr.type_name = "int";
      }
      col_reader_.insert(make_pair(x.colum_name, cr));
    }
  }

  void rescan()
  {
    row_ = 0;
  }

  Datum read_at_icol(std::string col_name)
  {
    Datum res;
    ColumnReader &cur_reader = col_reader_[col_name];
    std::string type_name = cur_reader.type_name;
    int cur_offset = cur_reader.start_offset + cur_reader.cur_rows * cur_reader.type_size;
    Seek(cur_offset, std::ios_base::beg);
    cur_reader.cur_rows++;
    if (type_name == "str")
    {
      uint8_t* data = ReadUInt8Array(32);
      std::string cur_str = std::string(reinterpret_cast<char*>(data));
      elog(LOG, "cur str is : %s", cur_str.c_str());
      res = CStringGetDatum(reinterpret_cast<char*>(data));
    }
    else if (type_name == "int")
    {
      void* four_data = allocator_->fast_alloc(4);
      Read4Bytes(reinterpret_cast<char *>(four_data));
      res = Int32GetDatum(*reinterpret_cast<int32_t*>(four_data));
    }
    else if (type_name == "float")
    {
      void* four_data = allocator_->fast_alloc(4);
      Read4Bytes(reinterpret_cast<char *>(four_data));
      res = Float4GetDatum(*reinterpret_cast<float4*>(four_data));
    }
    return res;
  }

private:
  uint32_t row_ = 0;      // cur_row
  uint32_t num_rows_ = 5; // total_rows
  std::vector<ColumnDesc> col_desc_;
  std::string file_path_; // file_path
  std::unordered_map<std::string, ColumnReader> col_reader_;
  std::unique_ptr<FastAllocator>  allocator_;
};

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
#endif