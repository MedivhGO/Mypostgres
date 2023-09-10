#ifndef DB721_FDW_COMMON_HPP
#define DB721_FDW_COMMON_HPP

#include <cstdarg>
#include <cstddef>
#include <iostream>
#include <exception>
#include <unordered_map>
#include <vector>

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

char *tolowercase(const char *input, char *output);
int32 string_to_int32(const char *s);

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


class DB721FileReader : public FileReader
{
public:

  DB721FileReader();
  ~DB721FileReader();

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
      elog(LOG, "fill tuple %d",slot->tts_tableOid);
      slot->tts_isnull[attr] = false;
    }
  }

  void rescan() {
    row_ = 0;
  }

  Datum read_primitive_type() {
    Datum res;
    uint32_t value = this->ReadUInt32();
    res = UInt32GetDatum(value);
    return res;
  }

  Datum read_at_pos(int begin_offset, int num_blocks, int number_of_value, std::string type_name) {
    Datum res;
    int cur_pos = begin_offset;
    while (number_of_value--) {
      this->Seek(cur_pos, std::ios_base::beg);
      if (type_name == "str") {
        cur_pos+=32;
        std::string values = this->ReadAsciiString(32);
        elog(LOG, "'%s'", values.c_str());
      } else if (type_name == "int") {
        cur_pos+=4;
        int32_t value;
        this->Read4Bytes(reinterpret_cast<char*>(&value));
        elog(LOG, "%d", value);
      } else if (type_name == "float") {
        float value;
        this->Read4Bytes(reinterpret_cast<char*>(&value));
        elog(LOG, "%f", value);
        cur_pos +=4;
      } 
    }
    return res;
  }

  void read_all_data();

private:
  uint32_t row_;                // cur_row
  uint32_t num_rows_;           // total_rows
  std::vector<ColumnDesc> col_desc_;
};

#endif