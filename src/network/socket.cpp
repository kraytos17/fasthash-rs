#include <sys/socket.h>
#include <utility>

#include "network/socket.hpp"

namespace fasthash {
    Socket::Socket(int fd) noexcept : m_fd(fd) {}

    Socket::~Socket() { reset(); }

    Socket::Socket(Socket&& other) noexcept : m_fd(std::exchange(other.m_fd, -1)) {}

    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            reset();
            m_fd = std::exchange(other.m_fd, -1);
        }
        return *this;
    }

    int Socket::get() const noexcept { return m_fd; }

    bool Socket::valid() const noexcept { return m_fd != -1; }

    int Socket::release() noexcept { return std::exchange(m_fd, -1); }

    void Socket::reset(int newFd) noexcept {
        if (m_fd != -1) {
            ::close(m_fd);
        }
        m_fd = newFd;
    }

    ssize_t Socket::send(std::span<const std::byte> data, int flags) const noexcept {
        return ::send(m_fd, data.data(), data.size_bytes(), flags);
    }

    ssize_t Socket::recv(std::span<std::byte> buffer, int flags) const noexcept {
        return ::recv(m_fd, buffer.data(), buffer.size_bytes(), flags);
    }
} // namespace fasthash
