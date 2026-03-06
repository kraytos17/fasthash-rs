#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace fasthash::byte_utils {
    template<std::integral T>
    constexpr void write_le(std::byte* dst, T value) noexcept {
        using U = std::make_unsigned_t<T>;
        U u = static_cast<U>(value);
        for (size_t i = 0; i < sizeof(T); ++i) {
            dst[i] = static_cast<std::byte>((u >> (i * 8)) & 0xFFu);
        }
    }

    template<std::integral T>
    constexpr void write_be(std::byte* dst, T value) noexcept {
        using U = std::make_unsigned_t<T>;
        U u = static_cast<U>(value);
        for (size_t i = 0; i < sizeof(T); ++i) {
            dst[sizeof(T) - 1 - i] = static_cast<std::byte>((u >> (i * 8)) & 0xFFu);
        }
    }

    constexpr void write_le24(std::byte* dst, std::integral auto value) noexcept {
        auto u = static_cast<uint32_t>(value);
        dst[0] = static_cast<std::byte>(u & 0xFFu);
        dst[1] = static_cast<std::byte>((u >> 8) & 0xFFu);
        dst[2] = static_cast<std::byte>((u >> 16) & 0xFFu);
    }

    constexpr void write_be32(std::byte* dst, uint32_t value) noexcept {
        dst[0] = static_cast<std::byte>((value >> 24) & 0xFFu);
        dst[1] = static_cast<std::byte>((value >> 16) & 0xFFu);
        dst[2] = static_cast<std::byte>((value >> 8) & 0xFFu);
        dst[3] = static_cast<std::byte>(value & 0xFFu);
    }

    template<std::integral T>
    [[nodiscard]] constexpr T read_le(const std::byte* src) noexcept {
        using U = std::make_unsigned_t<T>;
        U u{};
        for (size_t i = 0; i < sizeof(T); ++i) {
            u |= (U{std::to_integer<uint8_t>(src[i])} << (i * 8));
        }
        return static_cast<T>(u);
    }

    template<std::integral T>
    [[nodiscard]] constexpr T read_be(const std::byte* src) noexcept {
        using U = std::make_unsigned_t<T>;
        U u{};
        for (size_t i = 0; i < sizeof(T); ++i) {
            u |= (U{std::to_integer<uint8_t>(src[sizeof(T) - 1 - i])} << (i * 8));
        }
        return static_cast<T>(u);
    }

    [[nodiscard]] constexpr int32_t read_le24(const std::byte* src) noexcept {
        uint32_t u = (std::to_integer<uint32_t>(src[0])) |
                     (std::to_integer<uint32_t>(src[1]) << 8) |
                     (std::to_integer<uint32_t>(src[2]) << 16);

        if (u & 0x0080'0000u) {
            u |= 0xFF00'0000u;
        }
        return static_cast<int32_t>(u);
    }

    [[nodiscard]] constexpr uint32_t read_be32(const std::byte* src) noexcept {
        return (std::to_integer<uint32_t>(src[0]) << 24) |
               (std::to_integer<uint32_t>(src[1]) << 16) |
               (std::to_integer<uint32_t>(src[2]) << 8) | std::to_integer<uint32_t>(src[3]);
    }
}  // namespace fasthash::byte_utils
