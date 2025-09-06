#include "core/fast-hash.hpp"

#include <atomic>
#include <chrono>
#include <format>
#include <fstream>
#include <print>
#include <regex>
#include <shared_mutex>
#include <string>

#include "core/parser.hpp"

using json = nlohmann::json;
namespace chrono = std::chrono;

namespace fasthash {
    FastHash::FastHash(std::string_view aof_path, AOFSyncPolicy policy) :
        m_aofLogger(std::string(aof_path), policy) {
        m_ttlManager.set_expire_callback([this](const std::string& key) {
            std::scoped_lock lock(m_mtx);
            m_store.erase(key);
        });
    }

    FastHash::~FastHash() {
        stop();
        if (m_saveThread.joinable()) {
            m_saveThread.join();
        }
    }

    void FastHash::stop() {
        m_ttlManager.stop();
        if (m_saveThread.joinable()) {
            m_saveThread.request_stop();
        }
    }

    bool FastHash::set(std::string_view key, std::string_view value,
                       std::optional<int> ttl_seconds) {
        std::scoped_lock lock(m_mtx);
        std::string key_str{key};

        m_store.insert_or_assign(key_str, Value{std::string(value)});
        if (ttl_seconds.has_value()) {
            if (ttl_seconds.value() <= 0) {
                return false;
            }

            auto expire_time = chrono::steady_clock::now() + chrono::seconds(ttl_seconds.value());
            m_ttlManager.add_expiration(key_str, expire_time);
            if (!m_isAofLoading.load(std::memory_order_relaxed)) {
                m_aofLogger.log(std::format("SETEX {} {} {}", key, ttl_seconds.value(), value));
            }
        } else {
            m_ttlManager.remove_expiration(key_str);
            if (!m_isAofLoading.load(std::memory_order_relaxed)) {
                m_aofLogger.log(std::format("SET {} {}", key, value));
            }
        }
        return true;
    }

    std::optional<std::string> FastHash::get(std::string_view key) {
        std::shared_lock lock(m_mtx);
        std::string key_str{key};
        if (m_ttlManager.expired(key_str)) {
            lock.unlock();
            std::scoped_lock exclusive_lock(m_mtx);
            if (m_ttlManager.expired(key_str)) {
                m_store.erase(key_str);
                m_ttlManager.remove_expiration(key_str);
            }

            return std::nullopt;
        }

        if (auto it = m_store.find(key_str); it != m_store.end()) {
            return it->second.data;
        }

        return std::nullopt;
    }

    bool FastHash::del(std::string_view key) {
        std::scoped_lock lock(m_mtx);
        std::string key_str{key};
        bool erased = m_store.erase(key_str) > 0;
        if (erased) {
            m_ttlManager.remove_expiration(key_str);
            if (!m_isAofLoading.load(std::memory_order_relaxed)) {
                m_aofLogger.log(std::format("DEL {}", key));
            }
        }

        return erased;
    }

    bool FastHash::expire(std::string_view key, int seconds) {
        if (seconds <= 0) {
            return false;
        }

        std::scoped_lock lock(m_mtx);
        std::string key_str{key};
        if (!m_store.contains(key_str)) {
            return false;
        }

        auto expire_time = chrono::steady_clock::now() + chrono::seconds(seconds);
        m_ttlManager.add_expiration(key_str, expire_time);
        if (!m_isAofLoading.load(std::memory_order_relaxed)) {
            m_aofLogger.log(std::format("EXPIRE {} {}", key, seconds));
        }
        return true;
    }

    int FastHash::ttl(std::string_view key) {
        std::shared_lock lock(m_mtx);
        std::string key_str{key};
        if (!m_store.contains(key_str)) {
            return -2;
        }

        if (m_ttlManager.expired(key_str)) {
            lock.unlock();
            std::scoped_lock exclusive_lock(m_mtx);
            if (m_ttlManager.expired(key_str)) {
                m_store.erase(key_str);
                m_ttlManager.remove_expiration(key_str);
            }
            return -2;
        }

        if (auto expiry_opt = m_ttlManager.get_expiry_time(key_str); expiry_opt.has_value()) {
            auto now = chrono::steady_clock::now();
            auto remaining = chrono::duration_cast<chrono::seconds>(*expiry_opt - now).count();

            return remaining > 0 ? static_cast<int>(remaining) : -2;
        }
        return -1;
    }

    std::expected<std::string, std::string_view>
    FastHash::build_regex_pattern(std::string_view pattern) const {
        try {
            std::string regex_pattern = std::regex_replace(
                std::string{pattern}, std::regex{R"([\.\^\$\+\(\)\[\]\{\}])"}, R"(\$&)");

            regex_pattern = std::regex_replace(regex_pattern, std::regex{R"(\*)"}, ".*");
            regex_pattern = std::regex_replace(regex_pattern, std::regex{R"(\?)"}, ".");
            return "^" + regex_pattern + "$";
        } catch (...) {
            return std::unexpected("Invalid regex pattern");
        }
    }

    std::vector<std::string> FastHash::keys(std::string_view pattern) {
        auto regex_result = build_regex_pattern(pattern);
        if (!regex_result.has_value()) {
            return {};
        }

        std::shared_lock lock(m_mtx);
        std::vector<std::string> response;
        response.reserve(m_store.size());
        try {
            std::regex pattern_regex{regex_result.value()};
            for (const auto& [key, _]: m_store) {
                if (!m_ttlManager.expired(key) && std::regex_match(key, pattern_regex)) {
                    response.emplace_back(key);
                }
            }
        } catch (...) {
            return {};
        }

        lock.unlock();
        std::scoped_lock exclusive_lock(m_mtx);
        cleanup_expired_keys();

        return response;
    }

    void FastHash::cleanup_expired_keys() {
        std::erase_if(m_store, [this](const auto& kv) { return m_ttlManager.expired(kv.first); });
    }

    bool FastHash::exists(std::string_view key) {
        std::shared_lock lock(m_mtx);
        std::string key_str{key};

        if (m_ttlManager.expired(key_str)) {
            lock.unlock();
            std::scoped_lock exclusive_lock(m_mtx);
            if (m_ttlManager.expired(key_str)) {
                m_store.erase(key_str);
                m_ttlManager.remove_expiration(key_str);
            }
            return false;
        }
        return m_store.contains(key_str);
    }

    bool FastHash::persist(std::string_view key) {
        std::scoped_lock lock(m_mtx);
        std::string key_str{key};

        if (m_ttlManager.expired(key_str)) {
            m_store.erase(key_str);
            m_ttlManager.remove_expiration(key_str);
            return false;
        }

        if (!m_store.contains(key_str) || !m_ttlManager.has_expiration(key_str)) {
            return false;
        }

        m_ttlManager.remove_expiration(key_str);
        if (!m_isAofLoading.load(std::memory_order_relaxed)) {
            m_aofLogger.log(std::format("PERSIST {}", key));
        }
        return true;
    }

    void FastHash::flush_all() {
        std::scoped_lock lock(m_mtx);
        m_store.clear();
        m_ttlManager.clear_all();

        if (!m_isAofLoading.load(std::memory_order_relaxed)) {
            m_aofLogger.log("FLUSHALL");
        }
    }

    bool FastHash::save(std::string_view filename) const {
        json data;
        {
            std::shared_lock lock(m_mtx);
            data = serialize();
        }

        std::ofstream file{std::string{filename}};
        if (!file.is_open()) {
            std::print(stderr, "[ERROR] Failed to open file for SAVE: {}\n", filename);
            return false;
        }

        file << data.dump(2);
        return file.good();
    }

    bool FastHash::save_async(std::string_view filename) const {
        if (m_saveThread.joinable()) {
            m_saveThread.request_stop();
            m_saveThread.join();
        }

        m_saveThread =
            std::jthread([this, filename = std::string{filename}](std::stop_token token) {
                if (token.stop_requested()) {
                    return;
                }

                json data;
                {
                    std::shared_lock lock(m_mtx);
                    if (token.stop_requested()) {
                        return;
                    }
                    data = serialize();
                }

                if (token.stop_requested()) {
                    return;
                }

                std::ofstream file{filename};
                if (!file.is_open()) {
                    std::print(stderr, "[ERROR] Failed to open file for ASAVE: {}\n", filename);
                    return;
                }

                if (!token.stop_requested()) {
                    file << data.dump(2);
                }
            });
        return true;
    }

    json FastHash::serialize() const {
        json j;
        auto now = chrono::steady_clock::now();
        for (const auto& [key, val]: m_store) {
            json entry;
            entry["value"] = val.data;
            if (auto expiry_time = m_ttlManager.get_expiry_time(key); expiry_time.has_value()) {
                auto ttl = chrono::duration_cast<chrono::seconds>(*expiry_time - now).count();
                if (ttl > 0) {
                    entry["ttl"] = ttl;
                }
            }
            j[key] = std::move(entry);
        }
        return j;
    }

    bool FastHash::load(std::string_view filepath) {
        std::ifstream file{std::string{filepath}};
        if (!file.is_open()) {
            std::print(stderr, "[ERROR] Failed to open file for LOAD: {}\n", filepath);
            return false;
        }

        json data;
        try {
            file >> data;
        } catch (const std::exception& e) {
            std::print(stderr, "[ERROR] Failed to parse JSON: {}\n", e.what());
            return false;
        }

        std::scoped_lock lock(m_mtx);
        m_store.clear();
        m_ttlManager.clear_all();

        auto now = chrono::steady_clock::now();
        m_store.reserve(data.size());
        for (auto& [key, entry]: data.items()) {
            if (!entry.contains("value") || !entry["value"].is_string()) {
                std::print(stderr, "[ERROR] Invalid entry for key '{}', missing value\n", key);
                continue;
            }

            m_store.emplace(key, Value{entry["value"].get<std::string>()});
            if (entry.contains("ttl") && entry["ttl"].is_number_integer()) {
                int ttl_seconds = entry["ttl"];
                if (ttl_seconds > 0) {
                    auto expire_time = now + chrono::seconds(ttl_seconds);
                    m_ttlManager.add_expiration(key, expire_time);
                }
            }
        }
        return true;
    }

    void FastHash::replay_aof(std::string_view filepath) {
        std::ifstream file{std::string{filepath}};
        if (!file.is_open()) {
            std::print(stderr, "[WARN] AOF file not found: {}\n", filepath);
            return;
        }

        struct ScopedFlag {
            std::atomic_bool& flag;
            explicit ScopedFlag(std::atomic<bool>& f) : flag(f) {
                flag.store(true, std::memory_order_relaxed);
            }
            ~ScopedFlag() { flag.store(false, std::memory_order_relaxed); }
        } guard(m_isAofLoading);

        int count = 0;
        std::string line;
        while (std::getline(file, line)) {
            auto tokens = parser::tokenize(line);
            if (tokens.empty()) {
                continue;
            }

            auto cmd = parser::to_upper(tokens[0]);
            try {
                if (cmd == "SET" && tokens.size() == 3) {
                    set(tokens[1], tokens[2]);
                } else if (cmd == "SETEX" && tokens.size() == 4) {
                    set(tokens[1], tokens[3], std::stoi(tokens[2]));
                } else if (cmd == "DEL" && tokens.size() == 2) {
                    del(tokens[1]);
                } else if (cmd == "EXPIRE" && tokens.size() == 3) {
                    expire(tokens[1], std::stoi(tokens[2]));
                } else if (cmd == "PERSIST" && tokens.size() == 2) {
                    persist(tokens[1]);
                } else if (cmd == "FLUSHALL") {
                    flush_all();
                } else {
                    std::print(stderr, "[WARN] Unrecognized AOF command: {}\n", line);
                }
            } catch (...) {
                std::print(stderr, "[ERROR] Malformed AOF line: {}\n", line);
            }
            ++count;
        }

        std::print("[INFO] AOF replayed {} commands\n", count);
    }
} // namespace fasthash
