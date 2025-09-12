// Copyright 2025 Soubhik Gon
#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace fasthash {
    class TTLManager {
    public:
        using Clock = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;
        using Duration = Clock::duration;
        using ExpiryMap = std::unordered_map<std::string, TimePoint>;

        TTLManager();
        ~TTLManager();

        TTLManager(const TTLManager&) = delete;
        TTLManager& operator=(const TTLManager&) = delete;
        TTLManager(TTLManager&&) noexcept = delete;
        TTLManager& operator=(TTLManager&&) noexcept = delete;

        void add_expiration(std::string_view key, TimePoint expire_time);
        void remove_expiration(std::string_view key);
        void stop();
        void clear_all();

        [[nodiscard]] bool expired(std::string_view key);
        [[nodiscard]] bool has_expiration(std::string_view key) const;
        [[nodiscard]] std::optional<TimePoint> get_expiry_time(std::string_view key) const;

    private:
        struct ExpireEntry {
            std::string key;
            TimePoint expire_time;
            constexpr auto operator<=>(const ExpireEntry& other) const noexcept = default;
        };

        struct ExpireEntryComparator {
            constexpr bool operator()(const ExpireEntry& a, const ExpireEntry& b) const noexcept {
                return a.expire_time > b.expire_time;
            }
        };

        using ExpiryHeap =
            std::priority_queue<ExpireEntry, std::vector<ExpireEntry>, ExpireEntryComparator>;

        ExpiryHeap m_expiryHeap;
        ExpiryMap m_expiryMap;
        mutable std::mutex m_mtx;
        std::condition_variable_any m_cv;
        std::stop_source m_stopSource;
        std::jthread m_sweeperThread;

        void sweeper(std::stop_token token);
    };
}  // namespace fasthash
