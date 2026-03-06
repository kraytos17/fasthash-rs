// Copyright 2025 Soubhik Gon
#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace fasthash {
    namespace parser {
        [[nodiscard]] std::vector<std::string> tokenize(std::string_view line);
        [[nodiscard]] std::string to_upper(std::string_view str);
    }  // namespace parser
}  // namespace fasthash
