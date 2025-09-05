#pragma once

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace fasthash {
    /**
     * @brief Redis-compatible string implementation with multiple encodings
     *
     * Features:
     * - Small String Optimization (SSO) for strings <= 24 bytes
     * - Three encoding types: INT, FLOAT, and STRING
     * - Binary-safe operations with explicit length handling
     * - Full Redis API compatibility:
     *   - APPEND, INCR, INCRBYFLOAT
     *   - GETRANGE, SETRANGE
     *   - BITCOUNT, GETBIT, SETBIT
     *   - STRLEN, DEL
     * - Memory-efficient storage strategies
     */
    class FHString {
    public:
        /// Encoding types matching Redis implementation
        enum class Encoding : uint8_t {
            INT, ///< Stored as 64-bit integer
            RAW, ///< Stored as raw character buffer
            EMBSTR ///< Embedded short string (SSO)
        };

        FHString() noexcept;
        explicit FHString(const char* data, size_t size);
        explicit FHString(int64_t value) noexcept;
        explicit FHString(std::string_view str) noexcept;

        FHString(FHString&& other) noexcept;
        FHString& operator=(FHString&& other) noexcept;
        FHString(const FHString& other);
        FHString& operator=(const FHString& other);
        ~FHString();

        constexpr std::strong_ordering operator<=>(const FHString& other) const noexcept;
        bool operator==(const FHString& other) const noexcept;
        // explicit operator std::string_view() const noexcept;

        size_t append(const char* value, size_t len);
        int64_t increment(int64_t increment = 1);
        std::string_view get_range(size_t start, size_t end) const;
        void set_range(size_t offset, const char* data, size_t len);
        bool set_bit(size_t offset, bool on);
        bool get_bit(size_t offset) const;
        size_t bit_count(size_t start = 0, size_t end = -1) const;

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] bool empty() const noexcept;
        [[nodiscard]] const char* data() const noexcept;
        [[nodiscard]] char* data() noexcept;
        [[nodiscard]] Encoding encoding() const noexcept;
        [[nodiscard]] std::string_view view() const noexcept;
        [[nodiscard]] std::optional<int64_t> to_int() const noexcept;

        void reserve(size_t new_capacity);
        void shrink_to_fit();
        void clear() noexcept;
        std::optional<int64_t> from_chars_span() const noexcept;

    private:
        static constexpr size_t EMBSTR_LIMIT = 44;

        struct RawString {
            std::unique_ptr<char[]> ptr;
            size_t capacity;
            size_t length;

            RawString(std::unique_ptr<char[]> p, size_t cap, size_t len) :
                ptr(std::move(p)), capacity(cap), length(len) {}
        };

        std::array<char, EMBSTR_LIMIT> m_embstr;
        size_t m_embstr_len{0};
        union Storage {
            int64_t int_val;
            RawString raw_str_val;

            Storage() {}
            ~Storage() {}
        };

        Storage m_data;
        Encoding m_encoding;

        bool m_is_embstr;
        mutable std::array<char, 64> m_tmp_buf{};
        mutable size_t m_tmp_buf_len{0};

        void convert_to_raw();
        void ensure_capacity(size_t required);
        void materialize() const;
        void free_raw_string();

        unsigned char* byte_ptr(size_t byte_offset);
        const unsigned char* byte_ptr(size_t byte_offset) const;

        std::span<unsigned char> byte_span();
        std::span<const unsigned char> byte_span() const;
        std::span<unsigned char> get_bytes();
        std::span<const unsigned char> get_bytes() const;

        size_t normalize_index(ssize_t idx, size_t length) const noexcept;
        int64_t parse_int(const char* s, size_t len, bool& ok) const;
        long double parse_ld(const char* s, size_t len, bool& ok) const;
    };
} // namespace fasthash
