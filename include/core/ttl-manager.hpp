// Copyright 2025 Soubhik Gon
#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace chrono = std::chrono;

namespace fasthash {
    class TTLManager {
    public:
        TTLManager();
        ~TTLManager();

        TTLManager(const TTLManager&) = delete;
        TTLManager& operator=(const TTLManager&) = delete;
        TTLManager(TTLManager&&) noexcept = delete;
        TTLManager& operator=(TTLManager&&) noexcept = delete;

        void add_expiration(std::string_view key, chrono::steady_clock::time_point expire_time);
        void remove_expiration(std::string_view key);
        void stop();
        void clear_all();
        void set_expire_callback(std::function<void(const std::string&)> cb);

        [[nodiscard]] bool expired(std::string_view key);
        [[nodiscard]] bool has_expiration(std::string_view key) const;
        [[nodiscard]] std::optional<chrono::steady_clock::time_point>
        get_expiry_time(std::string_view key) const;

    private:
        struct ExpireEntry {
            std::string key;
            chrono::steady_clock::time_point expire_time;
            constexpr auto operator<=>(const ExpireEntry& other) const noexcept = default;
        };

        struct ExpireEntryComparator {
            constexpr bool operator()(const ExpireEntry& a, const ExpireEntry& b) const noexcept {
                return a.expire_time > b.expire_time;
            }
        };

        std::priority_queue<ExpireEntry, std::vector<ExpireEntry>, ExpireEntryComparator>
            m_expiryHeap;
        std::unordered_map<std::string, chrono::steady_clock::time_point> m_expiryMap;
        mutable std::mutex m_mtx;
        std::condition_variable_any m_cv;
        std::stop_source m_stopSource;
        std::jthread m_sweeperThread;

        void sweeper(std::stop_token token);
        std::move_only_function<void(const std::string&)> on_expire_callback;
    };
} // namespace fasthash
