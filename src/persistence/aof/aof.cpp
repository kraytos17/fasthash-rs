#include <format>
#include <print>
#include <unistd.h>

#include "persistence/aof/aof.hpp"

namespace fasthash {
    AOFLogger::AOFLogger(const std::string& path, AOFSyncPolicy policy) noexcept :
        m_syncPolicy(policy), m_lastSync(std::chrono::steady_clock::now()) {
        m_cFile = std::fopen(path.c_str(), "a");
        if (!m_cFile) {
            log_error(std::format("Failed to open AOF file: {}", path));
        }
    }

    AOFLogger::~AOFLogger() noexcept {
        if (m_cFile) {
            flush(true);
            std::fclose(m_cFile);
            m_cFile = nullptr;
        }
    }

    bool AOFLogger::log(const std::string& entry) noexcept {
        std::scoped_lock lock(m_mtx);
        if (!m_cFile) {
            log_error("AOF file handle is null (write skipped)");
            return false;
        }

        if (std::fprintf(m_cFile, "%s\n", entry.c_str()) < 0) {
            log_error(std::format("Failed to write entry to AOF: {}", entry));
            return false;
        }

        handle_sync_policy();
        return true;
    }

    bool AOFLogger::flush(bool force) noexcept {
        std::scoped_lock lock(m_mtx);
        if (!m_cFile) {
            log_error("AOF file handle is null (flush skipped)");
            return false;
        }
        if (std::fflush(m_cFile) != 0) {
            log_error("fflush() failed on AOF file");
            return false;
        }
        if (force || m_syncPolicy == AOFSyncPolicy::ALWAYS) {
            if (::fsync(::fileno(m_cFile)) != 0) {
                log_error("fsync() failed on AOF file");
                return false;
            }
        }
        return true;
    }

    void AOFLogger::handle_sync_policy() noexcept {
        switch (m_syncPolicy) {
            case AOFSyncPolicy::ALWAYS:
                flush(true);
                break;
            case AOFSyncPolicy::EVERYSEC: {
                auto now = std::chrono::steady_clock::now();
                if (now - m_lastSync >= std::chrono::seconds(1)) {
                    flush(true);
                    m_lastSync = now;
                }
                break;
            }
            case AOFSyncPolicy::NONE:
                // Do nothing, rely on OS flush
                break;
        }
    }

    void AOFLogger::log_error(const std::string& msg) const noexcept {
        std::print(stderr, "[AOF ERROR] {}\n", msg);
    }
} // namespace fasthash
