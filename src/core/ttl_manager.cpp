#include "core/ttl-manager.hpp"

TTLManager::TTLManager() : m_sweeperThread([this](std::stop_token token) { sweeper(token); }) {}

TTLManager::~TTLManager() { stop(); }

void TTLManager::stop() {
    m_stopSource.request_stop();
    m_cv.notify_all();
    if (m_sweeperThread.joinable()) {
        m_sweeperThread.join();
    }
}

void TTLManager::add_expiration(std::string_view key,
                                std::chrono::steady_clock::time_point expire_time) {
    {
        std::lock_guard lock(m_mtx);
        auto [it, inserted] = m_expiryMap.try_emplace(std::string{key}, expire_time);
        if (inserted || expire_time < it->second) {
            m_expiryHeap.emplace(it->first, expire_time);
            it->second = expire_time;
        }
    }
    m_cv.notify_one();
}

void TTLManager::remove_expiration(std::string_view key) {
    std::lock_guard lock(m_mtx);
    m_expiryMap.erase(std::string{key});
}

bool TTLManager::expired(std::string_view key) {
    std::lock_guard lock(m_mtx);
    if (auto it = m_expiryMap.find(std::string{key}); it != m_expiryMap.end()) {
        if (std::chrono::steady_clock::now() >= it->second) {
            m_expiryMap.erase(it);
            return true;
        }
    }

    return false;
}

bool TTLManager::has_expiration(std::string_view key) const {
    std::scoped_lock lock(m_mtx);
    return m_expiryMap.contains(std::string{key});
}

std::optional<std::chrono::steady_clock::time_point>
TTLManager::get_expiry_time(std::string_view key) const {
    std::scoped_lock lock(m_mtx);
    if (auto it = m_expiryMap.find(std::string{key}); it != m_expiryMap.end()) {
        return it->second;
    }

    return std::nullopt;
}

void TTLManager::clear_all() {
    {
        std::scoped_lock lock(m_mtx);
        m_expiryMap.clear();
        m_expiryHeap = decltype(m_expiryHeap){};
    }
    m_cv.notify_all();
}

void TTLManager::set_expire_callback(std::function<void(const std::string&)> cb) {
    std::scoped_lock s(m_mtx);
    on_expire_callback = std::move(cb);
}

void TTLManager::sweeper(std::stop_token token) {
    constexpr size_t kBatchSize = 100;
    constexpr auto kYieldDuration = std::chrono::milliseconds(1);

    std::unique_lock lock(m_mtx);
    while (!token.stop_requested()) {
        m_cv.wait(lock, [&] { return token.stop_requested() || !m_expiryHeap.empty(); });
        if (token.stop_requested()) {
            break;
        }

        size_t processed = 0;
        auto now = std::chrono::steady_clock::now();
        while (processed < kBatchSize && !m_expiryHeap.empty()) {
            const auto& next = m_expiryHeap.top();
            if (auto it = m_expiryMap.find(next.key);
                it == m_expiryMap.end() || it->second != next.expire_time) {
                m_expiryHeap.pop();
                continue;
            }

            if (now >= next.expire_time) {
                std::string expired_key = next.key;
                m_expiryHeap.pop();
                m_expiryMap.erase(expired_key);

                if (on_expire_callback) {
                    auto callback = std::move(on_expire_callback);
                    lock.unlock();
                    callback(expired_key);
                    lock.lock();
                }

                ++processed;
            } else {
                auto wait_duration = next.expire_time - now;
                m_cv.wait_for(lock, wait_duration, [&] { return token.stop_requested(); });
                break;
            }
        }

        if (processed == kBatchSize) {
            m_cv.wait_for(lock, kYieldDuration, [&] { return token.stop_requested(); });
        }
    }
}
