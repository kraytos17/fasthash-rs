#pragma once

#include <cstdint>
#include <mutex>
#include <netinet/in.h>
#include <thread>
#include <vector>

#include "network/socket.hpp"

namespace fasthash {
    /**
     * @brief Native TCP Server for FastHash
     */
    class TCPServer {
    public:
        explicit TCPServer(uint16_t port);
        ~TCPServer();

        TCPServer(const TCPServer&) = delete;
        TCPServer& operator=(const TCPServer&) = delete;
        TCPServer(TCPServer&&) noexcept = delete;
        TCPServer& operator=(TCPServer&&) noexcept = delete;

        void start();  ///< Launch accept loop in background
        void stop();  ///< Request stop + close socket

    private:
        uint16_t m_port;
        Socket m_serverSocket;
        std::jthread m_acceptThread;
        std::mutex m_clientsMutex;
        std::vector<std::jthread> m_clientThreads;

        void accept_clients(std::stop_token stoken);
        void handle_clients(std::stop_token stoken, Socket client);
    };
}  // namespace fasthash
