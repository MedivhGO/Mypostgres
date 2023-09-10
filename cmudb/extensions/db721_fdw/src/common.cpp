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

char *
tolowercase(const char *input, char *output)
{
	int i = 0;

	Assert(strlen(input) < NAMEDATALEN - 1);

	do
	{
		output[i] = tolower(input[i]);
	} while (input[i++]);

	return output;
}

int32 string_to_int32(const char *s)
{
	long l;
	char *badp;

	/*
	 * Some versions of strtol treat the empty string as an error, but some
	 * seem not to.  Make an explicit test to be sure we catch it.
	 */
	if (s == NULL)
		elog(ERROR, "NULL pointer");
	if (*s == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	errno = 0;
	l = strtol(s, &badp, 10);

	/* We made no progress parsing the string, so bail out */
	if (s == badp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
		/* won't get ERANGE on these with 64-bit longs... */
		|| l < INT_MIN || l > INT_MAX
#endif
	)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s", s,
						"integer")));

	/*
	 * Skip any trailing whitespace; if anything but whitespace remains before
	 * the terminating character, bail out
	 */
	while (*badp && *badp != '\0' && isspace((unsigned char)*badp))
		badp++;

	if (*badp && *badp != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	return (int32)l;
}

void DB721FileReader::read_all_data()
{
	std::vector<ColumnDesc> &test_colum = col_desc_;
	for (auto &item : test_colum)
	{
		std::string column_name = item.colum_name;
		int begin_offset = item.start_offset;
		int number_of_blocks = item.num_blocks;
		for (size_t i = 0; i < number_of_blocks; ++i)
		{
			int value_int_block = 0;
			std::string index_str = std::to_string(i);
			if (item.type_name == "float")
			{
				value_int_block = item.float_block_stat[index_str].value_in_block;
			}
			else if (item.type_name == "str")
			{
				value_int_block = item.str_block_stat[index_str].value_in_block;
			}
			else if (item.type_name == "int")
			{
				value_int_block = item.int_block_stat[index_str].value_in_block;
			}
			this->read_at_pos(begin_offset, number_of_blocks, value_int_block, item.type_name);
		}
	}
}