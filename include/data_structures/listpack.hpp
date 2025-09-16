#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

#include "../utils/byte_utils.hpp"

namespace fasthash {
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

    enum class ElemType : uint8_t { Int64, StrView };

    static constexpr uint16_t UnknownCount = 0xFFFF;
    static constexpr std::byte Terminator{static_cast<unsigned char>(0xFF)};
    static constexpr size_t HeaderSize = 6;

    struct Header {
        uint32_t tot_bytes;
        uint16_t num_elems;

        constexpr bool known() const noexcept { return num_elems != UnknownCount; }
    };

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

    class Listpack {
    public:
        class View {
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

                constexpr bool has_error() const noexcept { return m_err != Error::Ok; }
                constexpr Error error() const noexcept { return m_err; }

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

        class MutView {
        public:
            explicit MutView(std::span<std::byte> buf) noexcept : m_buffer(buf) {}

            std::expected<Header, Error> header() const noexcept;
            std::expected<void, Error> update_in_place(size_t index, ElemValue newval) noexcept;

        private:
            std::span<std::byte> m_buffer;
        };

        Listpack() {
            m_buffer = Buffer(HeaderSize + 1);  // header + terminator
            initialize_header();
        }

        explicit Listpack(size_t reserve) {
            m_buffer = Buffer(std::max(HeaderSize + 1, reserve));
            initialize_header();
        }

        std::span<const std::byte> data() const noexcept { return m_buffer.span(); }
        std::span<std::byte> data() noexcept { return m_buffer.span(); }

        View view() const noexcept { return View(m_buffer.span()); }
        MutView mut_view() noexcept { return MutView(m_buffer.span()); }

        std::expected<void, Error> push_back(ElemValue v);
        std::expected<void, Error> insert(size_t index, ElemValue v);
        std::expected<void, Error> erase(size_t index);

    private:
        class Buffer {
        public:
            Buffer() = default;
            explicit Buffer(size_t n) : m_data(std::make_unique<std::byte[]>(n)), m_size(n) {}

            void resize(size_t new_size) {
                auto new_data = std::make_unique<std::byte[]>(new_size);
                if (m_data) {
                    std::memcpy(new_data.get(), m_data.get(), std::min(m_size, new_size));
                }

                m_data = std::move(new_data);
                m_size = new_size;
            }

            std::byte* get() noexcept { return m_data.get(); }
            const std::byte* get() const noexcept { return m_data.get(); }

            std::span<std::byte> span() noexcept { return {m_data.get(), m_size}; }
            std::span<const std::byte> span() const noexcept { return {m_data.get(), m_size}; }

            size_t capacity() const noexcept { return m_size; }
            bool empty() const noexcept { return m_size == 0; }

        private:
            std::unique_ptr<std::byte[]> m_data;
            size_t m_size{0};
        };

        Buffer m_buffer;

        void initialize_header() noexcept {
            byte_utils::write_le(m_buffer.get() + 0, static_cast<uint32_t>(m_buffer.capacity()));
            byte_utils::write_le<uint16_t>(m_buffer.get() + 4, 0);
            m_buffer.get()[HeaderSize] = Terminator;
        }
    };

    namespace detail {
        struct EncodingHelpers {
            static constexpr uint8_t LP_ENCODING_7BIT_UINT_MASK = 0x80;
            static constexpr bool is_7bit_int_enc(uint8_t b) noexcept {
                return (b & LP_ENCODING_7BIT_UINT_MASK) == 0x00;
            }

            static constexpr uint8_t LP_6BIT_STR_TAG = 0x80;
            static constexpr uint8_t LP_6BIT_STR_MASK = 0xC0;
            static constexpr bool is_6bit_str_enc(uint8_t b) noexcept {
                return (b & LP_6BIT_STR_MASK) == LP_6BIT_STR_TAG;
            }
            static constexpr uint32_t len_6bit_str(uint8_t b) noexcept { return b & 0x3F; }

            static constexpr uint8_t LP_13BIT_INT_TAG = 0xC0;
            static constexpr uint8_t LP_13BIT_INT_MASK = 0xE0;
            static constexpr bool is_13bit_int_enc(uint8_t b) noexcept {
                return (b & LP_13BIT_INT_MASK) == LP_13BIT_INT_TAG;
            }

            static constexpr uint8_t LP_12BIT_STR_TAG = 0xE0;
            static constexpr uint8_t LP_12BIT_STR_MASK = 0xF0;
            static constexpr bool is_12bit_str_enc(uint8_t b) noexcept {
                return (b & LP_12BIT_STR_MASK) == LP_12BIT_STR_TAG;
            }
            static constexpr uint32_t len_12bit_str(uint8_t b0, uint8_t b1) noexcept {
                return (uint32_t(b0 & 0x0F) << 8) | uint32_t(b1);
            }

            static constexpr uint8_t LP_32BIT_STR_TAG = 0xF0;
            static constexpr bool is_32bit_str_enc(uint8_t b) noexcept {
                return b == LP_32BIT_STR_TAG;
            }
            static constexpr uint32_t len_32bit_str(const uint8_t* p) noexcept {
                return (uint32_t(p[1]) << 0) | (uint32_t(p[2]) << 8) | (uint32_t(p[3]) << 16) |
                       (uint32_t(p[4]) << 24);
            }

            static constexpr uint8_t LP_INT16 = 0xF1;
            static constexpr bool is_int16_enc(uint8_t b) noexcept { return b == LP_INT16; }

            static constexpr uint8_t LP_INT24 = 0xF2;
            static constexpr bool is_int24_enc(uint8_t b) noexcept { return b == LP_INT24; }

            static constexpr uint8_t LP_INT32 = 0xF3;
            static constexpr bool is_int32_enc(uint8_t b) noexcept { return b == LP_INT32; }

            static constexpr uint8_t LP_INT64 = 0xF4;
            static constexpr bool is_int64_enc(uint8_t b) noexcept { return b == LP_INT64; }

            static constexpr bool is_smallnum_string_byte(uint8_t b) noexcept {
                return (b & 0x80) == 0;
            }

            static constexpr uint8_t LP_ENCODING_7BIT_UINT = 0;
            static constexpr uint8_t LP_ENCODING_STRING_6BIT = 1;
            static constexpr uint8_t LP_ENCODING_STRING_12BIT = 2;
            static constexpr uint8_t LP_ENCODING_STRING_32BIT = 3;

            static constexpr size_t LP_ENCODING_6BIT_STR_ENTRY_SIZE = 1;
            static constexpr size_t LP_ENCODING_12BIT_STR_ENTRY_SIZE = 2;
            static constexpr size_t LP_ENCODING_32BIT_STR_ENTRY_SIZE = 5;
            static constexpr size_t LP_ENCODING_13BIT_INT_ENTRY_SIZE = 2;

            static constexpr bool fits_i13(int64_t v) noexcept;
            static constexpr bool fits_i16(int64_t v) noexcept;
            static constexpr bool fits_i24(int64_t v) noexcept;
            static constexpr bool fits_i32(int64_t v) noexcept;

            static std::optional<uint8_t> is_small_number_string(std::string_view s) noexcept;
            static size_t int_hp_len(int64_t v) noexcept;
            static size_t write_string_head_payload(std::byte* dst, std::string_view s) noexcept;
            static size_t write_int_head_payload(std::byte* dst, int64_t v) noexcept;

            struct StringEncodingInfo {
                int encoding;
                size_t hp_len;
                std::optional<uint8_t> smallnum;
            };

            static StringEncodingInfo string_encoding_and_hp_len(std::string_view s) noexcept;
        };

        struct BackLengthHelpers {
            static std::expected<std::pair<size_t, size_t>, Error> read_rtl(
                const std::byte* right, const std::byte* left_bound) noexcept;

            static void write_rtl(std::byte* end_ptr, size_t total_len) noexcept;

            static size_t calculate_size(size_t x) noexcept {
                size_t s = 1;
                while (x > ((size_t(1) << (7 * s)) - 1)) {
                    ++s;
                }
                return s;
            }
        };

        struct EntryHelpers {
            static std::expected<size_t, Error> entry_total_len(const std::byte* entry,
                                                                const std::byte* left_bound,
                                                                const std::byte* term) noexcept;

            static std::expected<const std::byte*, Error> advance_forward(
                const std::byte* cur, const std::byte* left, const std::byte* term) noexcept;

            static std::expected<const std::byte*, Error> advance_backward(
                const std::byte* cur, const std::byte* left) noexcept;

            static std::expected<const std::byte*, Error> get_entry_at_index(const std::byte* left,
                                                                             const std::byte* term,
                                                                             size_t index) noexcept;
        };

        struct ElementHelpers {
            static std::expected<size_t, Error> encode_elem(const ElemValue& v, std::byte* dst,
                                                            size_t capacity) noexcept;

            static std::expected<ElemValue, Error> decode_elem(const std::byte* entry,
                                                               const std::byte* left_bound,
                                                               const std::byte* term) noexcept;
        };

        struct HeaderHelpers {
            static std::expected<Header, Error> parse_header(
                std::span<const std::byte> buf) noexcept;
        };

    }  // namespace detail
}  // namespace fasthash
