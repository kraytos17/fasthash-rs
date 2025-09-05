#include <arpa/inet.h>
#include <array>
#include <print>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include "network/socket.hpp"
#include "network/tcp_server.hpp"

namespace {
    [[nodiscard]] std::string ip_to_string(const sockaddr_in& addr) noexcept {
        std::array<char, INET_ADDRSTRLEN> buf{};
        if (::inet_ntop(AF_INET, &addr.sin_addr, buf.data(), buf.size())) {
            return {buf.data()};
        }
        return "<invalid-ip>";
    }
} // namespace

TCPServer::TCPServer(std::uint16_t port) : m_port(port) {}

TCPServer::~TCPServer() { stop(); }

void TCPServer::start() {
    if (m_serverSocket.valid()) {
        throw std::runtime_error("Server already started");
    }

    Socket server(::socket(AF_INET, SOCK_STREAM, 0));
    if (!server.valid()) {
        throw std::system_error(errno, std::generic_category(), "socket() failed");
    }

    int opt = 1;
    if (::setsockopt(server.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::system_error(errno, std::generic_category(), "setsockopt() failed");
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(m_port);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(server.get(), reinterpret_cast<sockaddr*>(&serverAddr), sizeof(serverAddr)) < 0) {
        throw std::system_error(errno, std::generic_category(), "bind() failed");
    }

    if (::listen(server.get(), SOMAXCONN) < 0) {
        throw std::system_error(errno, std::generic_category(), "listen() failed");
    }

    m_serverSocket = std::move(server);
    std::print("[FastHash/TCP] Server started on port {}\n", m_port);
    m_acceptThread = std::jthread([this](std::stop_token stoken) { accept_clients(stoken); });
}

void TCPServer::stop() {
    if (m_serverSocket.valid()) {
        m_serverSocket.reset();
    }

    if (m_acceptThread.joinable()) {
        m_acceptThread.request_stop();
        m_acceptThread.join();
    }

    {
        std::scoped_lock lock(m_clientsMutex);
        for (auto& t: m_clientThreads) {
            if (t.joinable()) {
                t.request_stop();
                t.join();
            }
        }
        m_clientThreads.clear();
    }

    std::print("[FastHash/TCP] Server stopped.\n");
}

void TCPServer::accept_clients(std::stop_token stoken) {
    while (!stoken.stop_requested()) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);

        Socket client(
            ::accept(m_serverSocket.get(), reinterpret_cast<sockaddr*>(&clientAddr), &clientLen));

        if (!client.valid()) {
            if (stoken.stop_requested()) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }

            std::print(stderr,
                       "[FastHash/TCP] accept() failed: {} (errno={})\n",
                       std::generic_category().message(errno),
                       errno);
            continue;
        }

        std::print("[FastHash/TCP] New connection from {}\n", ip_to_string(clientAddr));

        {
            std::scoped_lock lock(m_clientsMutex);
            m_clientThreads.emplace_back(
                [client = std::move(client), this](std::stop_token ctoken) mutable {
                    handle_clients(ctoken, std::move(client));
                });
        }
    }
}

void TCPServer::handle_clients(std::stop_token stoken, Socket client) {
    constexpr size_t BUFFER_SIZE = 1024;
    std::array<std::byte, BUFFER_SIZE> buffer;

    while (!stoken.stop_requested()) {
        auto bytesRead = client.recv(buffer);
        if (bytesRead <= 0) {
            break;
        }

        std::string_view msg(reinterpret_cast<const char*>(buffer.data()),
                             static_cast<size_t>(bytesRead));

        std::print("[FastHash/TCP] Received: {}\n", msg);
        std::span<const std::byte> msg_bytes{buffer.data(), static_cast<size_t>(bytesRead)};
        client.send(msg_bytes);
    }

    std::print("[FastHash/TCP] Client disconnected.\n");
}
