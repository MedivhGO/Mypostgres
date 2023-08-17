#ifndef DB721_FDW_COMMON_HPP
#define DB721_FDW_COMMON_HPP

#include <cstdarg>
#include <cstddef>

extern "C"
{
#include "../../../../src/include/postgres.h"
#include "utils/jsonb.h"
};

#define ERROR_STR_LEN 512

struct Error : std::exception
{
    char text[ERROR_STR_LEN];

    Error(char const* fmt, ...) __attribute__((format(printf,2,3))) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(text, sizeof text, fmt, ap);
        va_end(ap);
    }

    char const* what() const throw() { return text; }
};

char *tolowercase(const char *input, char *output);
int32 string_to_int32(const char *s);

#endif