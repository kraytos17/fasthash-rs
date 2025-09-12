#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

namespace fasthash {
    namespace detail {
        inline constexpr uint8_t LP_ENCODING_7BIT_UINT_MASK = 0x80;
        inline constexpr bool is_7bit_int_enc(uint8_t b) noexcept {
            return (b & LP_ENCODING_7BIT_UINT_MASK) == 0x00;
        }

        inline constexpr uint8_t LP_6BIT_STR_TAG = 0x80;
        inline constexpr uint8_t LP_6BIT_STR_MASK = 0xC0;
        inline constexpr bool is_6bit_str_enc(uint8_t b) noexcept {
            return (b & LP_6BIT_STR_MASK) == LP_6BIT_STR_TAG;
        }

        inline constexpr uint32_t len_6bit_str(uint8_t b) noexcept { return b & 0x3F; }

        inline constexpr uint8_t LP_13BIT_INT_TAG = 0xC0;
        inline constexpr uint8_t LP_13BIT_INT_MASK = 0xE0;
        inline constexpr bool is_13bit_int_enc(uint8_t b) noexcept {
            return (b & LP_13BIT_INT_MASK) == LP_13BIT_INT_TAG;
        }

        inline constexpr uint8_t LP_12BIT_STR_TAG = 0xE0;
        inline constexpr uint8_t LP_12BIT_STR_MASK = 0xF0;
        inline constexpr bool is_12bit_str_enc(uint8_t b) noexcept {
            return (b & LP_12BIT_STR_MASK) == LP_12BIT_STR_TAG;
        }

        inline constexpr uint32_t len_12bit_str(uint8_t b0, uint8_t b1) noexcept {
            return (uint32_t(b0 & 0x0F) << 8) | uint32_t(b1);
        }

        inline constexpr uint8_t LP_INT16 = 0xF1;
        inline constexpr bool is_int16_enc(uint8_t b) noexcept { return b == LP_INT16; }

        inline constexpr uint8_t LP_INT24 = 0xF2;
        inline constexpr bool is_int24_enc(uint8_t b) noexcept { return b == LP_INT24; }

        inline constexpr uint8_t LP_INT32 = 0xF3;
        inline constexpr bool is_int32_enc(uint8_t b) noexcept { return b == LP_INT32; }

        inline constexpr uint8_t LP_INT64 = 0xF4;
        inline constexpr bool is_int64_enc(uint8_t b) noexcept { return b == LP_INT64; }

        inline constexpr uint8_t LP_32BIT_STR_TAG = 0xF0;
        inline constexpr bool is_32bit_str_enc(uint8_t b) noexcept { return b == LP_32BIT_STR_TAG; }
        inline constexpr uint32_t len_32bit_str(const uint8_t* p) noexcept {
            return (uint32_t(p[1]) << 0) | (uint32_t(p[2]) << 8) | (uint32_t(p[3]) << 16) |
                   (uint32_t(p[4]) << 24);
        }

        inline constexpr bool is_smallnum_string_byte(uint8_t b) noexcept {
            return (b & 0x80) == 0;
        }

        inline constexpr uint8_t LP_ENCODING_7BIT_UINT = 0;
        inline constexpr uint8_t LP_ENCODING_STRING_6BIT = 1;
        inline constexpr uint8_t LP_ENCODING_STRING_12BIT = 2;
        inline constexpr uint8_t LP_ENCODING_STRING_32BIT = 3;

        inline constexpr size_t LP_ENCODING_6BIT_STR_ENTRY_SIZE = 1;
        inline constexpr size_t LP_ENCODING_12BIT_STR_ENTRY_SIZE = 2;
        inline constexpr size_t LP_ENCODING_32BIT_STR_ENTRY_SIZE = 5;
        inline constexpr size_t LP_ENCODING_13BIT_INT_ENTRY_SIZE = 2;
    }  // namespace detail

    class Listpack {
    public:
        enum class Error {
            Ok = 0,
            Truncated,
            BadHeader,
            BadEncoding,
            BadLength,
            OutOfRange,
            NotFound,
            SizeMismatch
        };

        struct Header {
            uint32_t tot_bytes;  ///< Total size including header + entries + terminator
            uint16_t num_elems;  ///< Number of elements, or 0xFFFF if unknown
            bool known() const noexcept { return num_elems != UnknownCount; }
        };

        static constexpr uint16_t UnknownCount = 0xFFFF;
        static constexpr std::byte Terminator{static_cast<unsigned char>(0xFF)};

        enum class ElemType : uint8_t { Int64, StrView };

        struct ElemValue {
            ElemType type;
            union Data {
                int64_t i64;
                struct {
                    const char* ptr;
                    uint32_t len;
                } sv;

                Data() : i64(0) {}
            } data;

            static ElemValue make_int(int64_t v) noexcept {
                ElemValue e;
                e.type = ElemType::Int64;
                e.data.i64 = v;
                return e;
            }
            static ElemValue make_str(const char* p, uint32_t n) noexcept {
                ElemValue e;
                e.type = ElemType::StrView;
                e.data.sv = {p, n};
                return e;
            }

            int64_t as_int() const noexcept { return data.i64; }
            std::string_view as_str() const noexcept { return {data.sv.ptr, data.sv.len}; }
        };

        struct Buffer {
            std::unique_ptr<std::byte[]> data;
            size_t size{0};

            Buffer() = default;

            explicit Buffer(size_t n) : data(std::make_unique<std::byte[]>(n)), size(n) {}

            void resize(size_t new_size) {
                auto new_data = std::make_unique<std::byte[]>(new_size);
                if (data) {
                    std::memcpy(new_data.get(), data.get(), std::min(size, new_size));
                }

                data = std::move(new_data);
                size = new_size;
            }

            std::byte* get() noexcept { return data.get(); }
            const std::byte* get() const noexcept { return data.get(); }

            std::span<std::byte> span() noexcept { return {data.get(), size}; }
            std::span<const std::byte> span() const noexcept { return {data.get(), size}; }
        };

        class View;
        class MutView;
        class Pack;

        static constexpr uint16_t read_u16_le(const std::byte* p) noexcept {
            return static_cast<uint16_t>(uint16_t(p[0]) | (uint16_t(p[1]) << 8));
        }

        static constexpr uint32_t read_u32_le(const std::byte* p) noexcept {
            return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) |
                   (uint32_t(p[3]) << 24);
        }

        static constexpr void write_u16_le(std::byte* p, uint16_t v) noexcept {
            p[0] = std::byte(v & 0xFF);
            p[1] = std::byte((v >> 8) & 0xFF);
        }

        static constexpr void write_u32_le(std::byte* p, uint32_t v) noexcept {
            p[0] = std::byte(v & 0xFF);
            p[1] = std::byte((v >> 8) & 0xFF);
            p[2] = std::byte((v >> 16) & 0xFF);
            p[3] = std::byte((v >> 24) & 0xFF);
        }
    };

    class Listpack::View {
    public:
        explicit View(std::span<const std::byte> buf) noexcept : m_buffer(buf) {}

        std::expected<Header, Error> header() const noexcept;
        std::expected<size_t, Error> size() const noexcept;

        class Iterator {
        public:
            using iterator_concept = std::bidirectional_iterator_tag;
            using value_type = ElemValue;
            using difference_type = ptrdiff_t;
            using pointer = const ElemValue*;
            using reference = const ElemValue&;

            Iterator() = default;
            Iterator(const std::byte* base, const std::byte* cur, const std::byte* left_bound,
                     const std::byte* term);

            reference operator*() const noexcept { return m_current; }
            pointer operator->() const noexcept { return &m_current; }

            Iterator& operator++();
            Iterator operator++(int);

            Iterator& operator--();
            Iterator operator--(int);

            constexpr friend bool operator==(const Iterator& a, const Iterator& b) noexcept {
                return a.m_cur == b.m_cur;
            }

            constexpr friend bool operator!=(const Iterator& a, const Iterator& b) noexcept {
                return !(a == b);
            }

            constexpr friend auto operator<=>(const Iterator& a, const Iterator& b) noexcept {
                return a.m_cur <=> b.m_cur;
            }

            bool has_error() const noexcept { return m_err != Error::Ok; }
            Error error() const noexcept { return m_err; }

        private:
            const std::byte* m_base{};
            const std::byte* m_cur{};
            const std::byte* m_left_bound{};
            const std::byte* m_term{};
            ElemValue m_current{};
            Error m_err{Error::Ok};

            void decode_current() noexcept;
        };

        Iterator begin() const noexcept;
        Iterator end() const noexcept;

    private:
        std::span<const std::byte> m_buffer;
    };

    class Listpack::MutView {
    public:
        explicit MutView(std::span<std::byte> buf) noexcept : m_buffer(buf) {}

        std::expected<Header, Error> header() const noexcept;
        std::expected<void, Error> update_in_place(size_t index, ElemValue newval) noexcept;

    private:
        std::span<std::byte> m_buffer;
    };

    class Listpack::Pack {
    public:
        Pack() {
            m_buffer = Buffer(HeaderSize + 1);  // header + terminator
            write_u32_le(m_buffer.get() + 0, HeaderSize + 1);
            write_u16_le(m_buffer.get() + 4, 0);
            m_buffer.get()[HeaderSize] = Terminator;
        }

        explicit Pack(size_t reserve) {
            m_buffer = Buffer(std::max(HeaderSize + 1, reserve));
            write_u32_le(m_buffer.get() + 0, static_cast<uint32_t>(m_buffer.size));
            write_u16_le(m_buffer.get() + 4, 0);
            m_buffer.get()[HeaderSize] = Terminator;
        }

        std::span<const std::byte> data() const noexcept { return m_buffer.span(); }
        std::span<std::byte> data() noexcept { return m_buffer.span(); }

        View view() const noexcept { return View(m_buffer.span()); }
        MutView mut_view() noexcept { return MutView(m_buffer.span()); }

        std::expected<void, Error> push_back(ElemValue v);
        std::expected<void, Error> insert(size_t index, ElemValue v);
        std::expected<void, Error> erase(size_t index);

    private:
        Buffer m_buffer;
        static constexpr size_t HeaderSize = 6;
    };
}  // namespace fasthash
