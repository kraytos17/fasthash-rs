// Copyright 2025 Soubhik Gon
// fast-hash.hpp
#pragma once

#include <atomic>
#include <expected>
#include <nlohmann/json.hpp>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "persistence/aof/aof.hpp"
#include "ttl-manager.hpp"

namespace fasthash {
    class FastHash {
    public:
        explicit FastHash(std::string_view aof_path = "appendonly.aof",
                          AOFSyncPolicy policy = AOFSyncPolicy::ALWAYS);

        ~FastHash();
        FastHash(const FastHash&) = delete;
        FastHash& operator=(const FastHash&) = delete;
        FastHash(FastHash&&) = delete;
        FastHash& operator=(FastHash&&) = delete;

        bool set(std::string_view key, std::string_view value,
                 std::optional<int> ttl_seconds = std::nullopt);

        std::optional<std::string> get(std::string_view key);
        bool del(std::string_view key);
        bool expire(std::string_view key, int seconds);
        int ttl(std::string_view key);
        std::vector<std::string> keys(std::string_view pattern = "*");
        bool exists(std::string_view key);
        bool persist(std::string_view key);
        bool save(std::string_view filename = "dump.json") const;
        bool save_async(std::string_view filename = "dump.json") const;
        nlohmann::json serialize() const;
        bool load(std::string_view filepath = "dump.json");
        void replay_aof(std::string_view filepath);
        void stop();
        void flush_all();

    private:
        struct Value {
            std::string data;
            explicit Value(std::string d) : data(std::move(d)) {}
        };

        std::unordered_map<std::string, Value> m_store;
        TTLManager m_ttlManager;
        mutable std::shared_mutex m_mtx;
        AOFLogger m_aofLogger;
        std::atomic_bool m_isAofLoading{false};
        mutable std::jthread m_saveThread;

        std::expected<std::string, std::string_view>
        build_regex_pattern(std::string_view pattern) const;

        void cleanup_expired_keys();
    };
} // namespace fasthash
