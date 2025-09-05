#pragma once

#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>

enum class AOFSyncPolicy { ALWAYS, EVERYSEC, NONE };

class AOFLogger {
public:
    explicit AOFLogger(const std::string& path,
                       AOFSyncPolicy policy = AOFSyncPolicy::ALWAYS) noexcept;
    ~AOFLogger() noexcept;

    AOFLogger(const AOFLogger&) = delete;
    AOFLogger& operator=(const AOFLogger&) = delete;
    AOFLogger(AOFLogger&&) noexcept = delete;
    AOFLogger& operator=(AOFLogger&&) noexcept = delete;

    bool log(const std::string& entry) noexcept;
    bool flush(bool force = false) noexcept;

private:
    void handle_sync_policy() noexcept;
    void log_error(const std::string& msg) const noexcept;

    FILE* m_cFile{};
    std::mutex m_mtx;
    AOFSyncPolicy m_syncPolicy{AOFSyncPolicy::ALWAYS};
    std::chrono::steady_clock::time_point m_lastSync;
};
