#pragma once

#include <variant>
#include <vector>
#include <unordered_map>
#include <string>
#include <string_view>
#include <optional>
#include <regex>
#include <charconv>
#include <memory>

namespace jsontuil {

struct JSONObject;

using JSONDict = std::unordered_map<std::string, std::unique_ptr<JSONObject>>;
using JSONList = std::vector<JSONObject>;

struct JSONObject {
    std::variant
    < std::nullptr_t  // null
    , bool            // true
    , int             // 42
    , double          // 3.14
    , std::string     // "hello"
    , JSONList        // [42, "hello"]
    , JSONDict        // {"hello": 985, "world": 211}
    > inner;
};

JSONObject parse(std::string_view json) {
    if (json.empty()) {
        return JSONObject{std::nullptr_t{}};
    }
    if ('0' <= json[0] && json[0] <= '9') {
        int val = json[0];
        return JSONObject{int{val - '0'}};
    }
    return JSONObject{std::nullptr_t{}};
}



} // end jsonutil namespace



