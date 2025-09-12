// Copyright 2025 Soubhik Gon
#include "core/parser.hpp"

#include <algorithm>
#include <cctype>
#include <vector>

namespace fasthash {
    namespace parser {
        std::vector<std::string> tokenize(std::string_view line) {
            std::vector<std::string> tokens;
            tokens.reserve(8);

            auto start = line.begin();
            auto end = line.end();
            while (start != end) {
                start =
                    std::find_if_not(start, end, [](unsigned char c) { return std::isspace(c); });
                if (start == end) {
                    break;
                }

                auto token_end =
                    std::find_if(start, end, [](unsigned char c) { return std::isspace(c); });

                tokens.emplace_back(start, token_end);
                start = token_end;
            }

            return tokens;
        }

        std::string to_upper(std::string_view str) {
            std::string result;
            result.reserve(str.size());
            std::transform(str.begin(), str.end(), std::back_inserter(result), [](unsigned char c) {
                return std::toupper(c);
            });

            return result;
        }
    }  // namespace parser
}  // namespace fasthash
