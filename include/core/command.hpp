// Copyright 2025 Soubhik Gon
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "fast-hash.hpp"

namespace fasthash {
    /**
     * @brief Represents a parsed command and its arguments.
     */
    class Command {
    public:
        enum class Type {
            SET,
            SETEX,
            GET,
            DEL,
            EXPIRE,
            TTL,
            KEYS,
            EXISTS,
            PERSIST,
            FLUSHALL,
            SAVE,
            ASAVE,
            LOAD,
            INVALID
        };

        Type type{Type::INVALID};
        std::vector<std::string> args;

        [[nodiscard]] static Command parse(std::string_view line);
        void execute(FastHash& store) const;
    };
}  // namespace fasthash
