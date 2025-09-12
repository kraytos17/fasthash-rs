#pragma once

#include <cstddef>
#include <span>
#include <unistd.h>

namespace fasthash {
    /**
     * @brief RAII wrapper for a POSIX socket file descriptor.
     *
     * Ensures that the socket is closed automatically when the object
     * goes out of scope, unless ownership has been released.
     */
    class Socket {
    public:
        /// Construct with an optional file descriptor (default -1 means invalid).
        explicit Socket(int fd = -1) noexcept;

        /// Destructor closes the socket if still owned.
        ~Socket();

        // Non-copyable
        Socket(const Socket&) = delete;
        Socket& operator=(const Socket&) = delete;

        // Movable
        Socket(Socket&& other) noexcept;
        Socket& operator=(Socket&& other) noexcept;

        /// @return the raw file descriptor.
        [[nodiscard]] int get() const noexcept;

        /// @return true if this socket is valid.
        [[nodiscard]] bool valid() const noexcept;

        /// Release ownership of the socket and return the file descriptor.
        int release() noexcept;

        /// Reset the socket (close current if valid, replace with newFd).
        void reset(int newFd = -1) noexcept;

        /// Send data from a byte span
        ssize_t send(std::span<const std::byte> data, int flags = 0) const noexcept;

        /// Receive data into a byte span
        ssize_t recv(std::span<std::byte> buffer, int flags = 0) const noexcept;

    private:
        int m_fd{-1};
    };
}  // namespace fasthash
