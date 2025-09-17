#include "data_structures/string.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <cmath>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace fasthash {
    inline std::string to_chars_string(int64_t value) {
        std::array<char, 64> buf;
        auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
        if (ec != std::errc{}) {
            throw std::runtime_error("to_chars_string failed");
        }
        return std::string(buf.data(), ptr - buf.data());
    }

    inline size_t to_chars_size(int64_t value) {
        std::array<char, 64> buf;
        auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
        if (ec != std::errc{}) {
            throw std::runtime_error("to_chars_size failed");
        }
        return ptr - buf.data();
    }

    FHString::FHString() noexcept : m_embstr_len(0), m_encoding(Encoding::RAW), m_is_embstr(false) {
        m_data.int_val = 0;
    }

    FHString::FHString(std::string_view str) noexcept
      : m_embstr_len(0), m_encoding(Encoding::RAW), m_is_embstr(false) {
        if (str.size() <= EMBSTR_LIMIT) {
            std::copy(str.begin(), str.end(), m_embstr.begin());
            m_embstr_len = str.size();
            m_encoding = Encoding::EMBSTR;
            m_is_embstr = true;
        } else {
            std::construct_at(
                &m_data.raw_str_val, std::make_unique<char[]>(str.size()), str.size(), str.size());

            std::copy(str.begin(), str.end(), m_data.raw_str_val.ptr.get());
            m_encoding = Encoding::RAW;
            m_is_embstr = false;
        }
    }

    FHString::FHString(const char* data, size_t size) : FHString(std::string_view(data, size)) {}
    FHString::FHString(int64_t value) noexcept : m_encoding(Encoding::INT), m_is_embstr(false) {
        m_data.int_val = value;
    }

    FHString::FHString(const FHString& other)
      : m_embstr_len(0), m_encoding(Encoding::RAW), m_is_embstr(false) {
        if (other.m_is_embstr) {
            std::copy_n(other.m_embstr.begin(), other.m_embstr_len, m_embstr.begin());
            m_embstr_len = other.m_embstr_len;
            m_encoding = Encoding::EMBSTR;
            m_is_embstr = true;
        } else {
            m_is_embstr = false;
            m_encoding = other.m_encoding;
            if (other.m_encoding == Encoding::INT) {
                m_data.int_val = other.m_data.int_val;
            } else if (other.m_encoding == Encoding::RAW) {
                const RawString& o_raw = other.m_data.raw_str_val;
                std::construct_at(&m_data.raw_str_val,
                                  std::make_unique<char[]>(o_raw.capacity),
                                  o_raw.capacity,
                                  o_raw.length);

                std::copy_n(o_raw.ptr.get(), o_raw.length, m_data.raw_str_val.ptr.get());
            }
        }
    }

    FHString& FHString::operator=(const FHString& other) {
        if (this == &other) {
            return *this;
        }
        if (!m_is_embstr && m_encoding == Encoding::RAW) {
            std::destroy_at(&m_data.raw_str_val);
        }
        if (other.m_is_embstr) {
            std::copy_n(other.m_embstr.begin(), other.m_embstr_len, m_embstr.begin());
            m_embstr_len = other.m_embstr_len;
            m_is_embstr = true;
            m_encoding = other.m_encoding;
        } else {
            m_is_embstr = false;
            m_encoding = other.m_encoding;
            m_embstr_len = 0;
            if (other.m_encoding == Encoding::INT) {
                m_data.int_val = other.m_data.int_val;
            } else if (other.m_encoding == Encoding::RAW) {
                const RawString& o_raw = other.m_data.raw_str_val;
                std::construct_at(&m_data.raw_str_val,
                                  std::make_unique<char[]>(o_raw.capacity),
                                  o_raw.capacity,
                                  o_raw.length);

                std::copy_n(o_raw.ptr.get(), o_raw.length, m_data.raw_str_val.ptr.get());
            }
        }

        return *this;
    }

    FHString::FHString(FHString&& other) noexcept
      : m_embstr_len(0), m_encoding(other.m_encoding), m_is_embstr(other.m_is_embstr) {
        if (m_is_embstr) {
            std::copy_n(other.m_embstr.begin(), other.m_embstr_len, m_embstr.begin());
            m_embstr_len = other.m_embstr_len;
        } else {
            if (other.m_encoding == Encoding::INT) {
                m_data.int_val = other.m_data.int_val;
            } else if (other.m_encoding == Encoding::RAW) {
                std::construct_at(&m_data.raw_str_val, std::move(other.m_data.raw_str_val));
            }
        }

        other.m_is_embstr = false;
        other.m_encoding = Encoding::RAW;
        other.m_embstr_len = 0;
    }

    FHString& FHString::operator=(FHString&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        if (!m_is_embstr && m_encoding == Encoding::RAW) {
            std::destroy_at(&m_data.raw_str_val);
        }

        m_is_embstr = other.m_is_embstr;
        m_encoding = other.m_encoding;
        m_embstr_len = 0;
        if (m_is_embstr) {
            std::copy_n(other.m_embstr.begin(), other.m_embstr_len, m_embstr.begin());
            m_embstr_len = other.m_embstr_len;
        } else {
            if (other.m_encoding == Encoding::INT) {
                m_data.int_val = other.m_data.int_val;
            } else if (other.m_encoding == Encoding::RAW) {
                std::construct_at(&m_data.raw_str_val, std::move(other.m_data.raw_str_val));
            }
        }

        other.m_is_embstr = false;
        other.m_encoding = Encoding::RAW;
        other.m_embstr_len = 0;

        return *this;
    }

    FHString::~FHString() {
        if (m_encoding == Encoding::RAW) {
            std::destroy_at(&m_data.raw_str_val);
        }
    }

    constexpr std::strong_ordering fasthash::FHString::operator<=>(
        const FHString& other) const noexcept {
        if (m_encoding == Encoding::INT && other.m_encoding == Encoding::INT) {
            return m_data.int_val <=> other.m_data.int_val;
        }

        auto lhs_span = byte_span();
        auto rhs_span = other.byte_span();
        size_t min_len = std::min(lhs_span.size(), rhs_span.size());
        auto cmp = std::lexicographical_compare_three_way(lhs_span.begin(),
                                                          lhs_span.begin() + min_len,
                                                          rhs_span.begin(),
                                                          rhs_span.begin() + min_len,
                                                          std::compare_three_way{});

        if (cmp != std::strong_ordering::equal) {
            return cmp;
        }

        return lhs_span.size() <=> rhs_span.size();
    }

    bool fasthash::FHString::operator==(const FHString& other) const noexcept {
        if (m_encoding == Encoding::INT && other.m_encoding == Encoding::INT) {
            return m_data.int_val == other.m_data.int_val;
        }

        auto lhs_span = byte_span();
        auto rhs_span = other.byte_span();

        return lhs_span.size() == rhs_span.size() &&
               std::equal(lhs_span.begin(), lhs_span.end(), rhs_span.begin());
    }

    size_t FHString::append(const char* value, size_t len) {
        if (m_encoding == Encoding::INT) {
            bool all_digits =
                std::all_of(value, value + len, [](char c) { return std::isdigit(c); });

            if (all_digits) {
                auto int_str = to_chars_string(m_data.int_val);
                std::string combined = int_str;
                combined.append(value, len);

                auto new_val_opt = parse_int(combined.data(), combined.size());
                if (new_val_opt) {
                    m_data.int_val = new_val_opt.value();
                    return combined.size();
                }
            }

            materialize();
        }
        if (m_is_embstr) {
            if (m_embstr_len + len <= EMBSTR_LIMIT) {
                std::copy_n(value, len, m_embstr.data() + m_embstr_len);
                m_embstr_len += len;
                return m_embstr_len;
            }
            convert_to_raw();
        }
        if (m_encoding == Encoding::RAW) {
            RawString& raw = m_data.raw_str_val;
            ensure_capacity(raw.length + len);
            std::copy_n(value, len, raw.ptr.get() + raw.length);
            raw.length += len;
            return raw.length;
        }

        return 0;
    }

    int64_t FHString::increment(int64_t inc) {
        auto safe_add = [](int64_t a, int64_t b, bool& overflow) {
            if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b)) {
                overflow = true;
                return int64_t{0};
            }
            overflow = false;
            return a + b;
        };

        if (m_encoding == Encoding::INT) {
            int64_t& val = m_data.int_val;
            bool overflow = false;
            auto res = safe_add(val, inc, overflow);
            if (overflow) {
                throw std::overflow_error("increment would overflow");
            }

            val = res;
            return val;
        } else {
            auto res = from_chars_span()
                           .transform([&](int64_t v) {
                               bool overflow = false;
                               auto r = safe_add(v, inc, overflow);
                               if (overflow) {
                                   throw std::overflow_error("increment would overflow");
                               }
                               return r;
                           })
                           .or_else([] {
                               throw std::runtime_error("Value is not an integer");
                               return std::optional<int64_t>{};
                           })
                           .value();

            if (!m_is_embstr && m_encoding == Encoding::RAW) {
                std::destroy_at(&m_data.raw_str_val);
            }

            m_data.int_val = res;
            m_encoding = Encoding::INT;
            m_is_embstr = false;
            return res;
        }
    }

    std::string_view FHString::get_range(size_t start, size_t end) const {
        const auto bytes = byte_span();
        if (bytes.empty()) {
            return {};
        }

        size_t len = bytes.size();
        size_t real_start = normalize_index(start, len);
        size_t real_end = normalize_index(end, len);
        if (real_start > real_end) {
            return {};
        }

        return std::string_view(reinterpret_cast<const char*>(bytes.data() + real_start),
                                real_end - real_start + 1);
    }

    void FHString::set_range(size_t offset, const char* data, size_t len) {
        materialize();
        size_t str_len = size();
        size_t real_offset = normalize_index(offset, str_len);
        size_t new_len = std::max(str_len, real_offset + len);
        if (m_is_embstr && new_len <= EMBSTR_LIMIT) {
            std::copy_n(data, len, m_embstr.data() + real_offset);
            m_embstr_len = new_len;
            return;
        }

        convert_to_raw();
        RawString& raw = m_data.raw_str_val;
        ensure_capacity(new_len);
        std::copy_n(data, len, raw.ptr.get() + real_offset);
        raw.length = new_len;
    }

    bool FHString::set_bit(size_t offset, bool on) {
        size_t byte_pos = offset / 8;
        size_t str_len = size();
        if (m_encoding != Encoding::RAW && byte_pos >= str_len) {
            materialize();
            str_len = size();
        }

        if (byte_pos >= str_len) {
            ensure_capacity(byte_pos + 1);
            size_t num_new_bytes = byte_pos + 1 - str_len;
            if (m_is_embstr) {
                std::fill_n(m_embstr.begin() + m_embstr_len, num_new_bytes, 0);
                m_embstr_len = byte_pos + 1;
            } else {
                RawString& raw = m_data.raw_str_val;
                std::fill_n(raw.ptr.get() + raw.length, num_new_bytes, 0);
                raw.length = byte_pos + 1;
            }
        }

        auto* byte = byte_ptr(byte_pos);
        bool old = (*byte >> (7 - (offset % 8))) & 1;
        if (on) {
            *byte |= (1 << (7 - (offset % 8)));
        } else {
            *byte &= ~(1 << (7 - (offset % 8)));
        }

        return old;
    }

    bool FHString::get_bit(size_t offset) const {
        const auto bytes = byte_span();
        if (offset / 8 >= bytes.size()) {
            return false;
        }

        size_t byte_pos = offset / 8;
        size_t bit_pos = 7 - (offset % 8);

        return (bytes[byte_pos] >> bit_pos) & 1;
    }

    size_t FHString::bit_count(size_t start, size_t end) const {
        const auto bytes = byte_span();
        if (start >= bytes.size()) {
            return 0;
        }

        end = std::min(end, bytes.size() - 1);
        return std::accumulate(bytes.begin() + start,
                               bytes.begin() + end + 1,
                               size_t{0},
                               [](size_t acc, unsigned char b) { return acc + std::popcount(b); });
    }

    size_t FHString::size() const noexcept {
        switch (m_encoding) {
            case Encoding::INT:
                return to_chars_size(m_data.int_val);
            case Encoding::RAW:
                return m_data.raw_str_val.length;
            case Encoding::EMBSTR:
                return m_is_embstr ? m_embstr_len : 0;
            default:
                return 0;
        }
    }

    bool FHString::empty() const noexcept { return size() == 0; }

    const char* FHString::data() const noexcept {
        switch (m_encoding) {
            case Encoding::EMBSTR:
                return m_embstr.data();
            case Encoding::RAW:
                return m_data.raw_str_val.ptr.get();
            case Encoding::INT: {
                auto [ptr, ec] = std::to_chars(
                    m_tmp_buf.data(), m_tmp_buf.data() + m_tmp_buf.size(), m_data.int_val);
                if (ec != std::errc{}) {
                    return nullptr;
                }

                m_tmp_buf_len = ptr - m_tmp_buf.data();
                return m_tmp_buf.data();
            }
        }
        return nullptr;
    }

    char* FHString::data() noexcept {
        materialize();
        if (m_is_embstr) {
            return m_embstr.data();
        }
        if (m_encoding == Encoding::RAW) {
            return m_data.raw_str_val.ptr.get();
        }
        return nullptr;
    }

    FHString::Encoding FHString::encoding() const noexcept { return m_encoding; }

    std::optional<int64_t> FHString::to_int() const noexcept { return from_chars_span(); }

    void FHString::reserve(size_t new_capacity) {
        materialize();
        if (m_is_embstr && new_capacity <= EMBSTR_LIMIT) {
            return;
        }

        convert_to_raw();
        ensure_capacity(new_capacity);
    }

    void FHString::shrink_to_fit() {
        materialize();
        if (m_is_embstr) {
            return;
        }

        if (m_encoding == Encoding::RAW) {
            RawString& raw = m_data.raw_str_val;
            if (raw.length == raw.capacity) {
                return;
            }
            if (raw.length <= EMBSTR_LIMIT) {
                std::copy_n(raw.ptr.get(), raw.length, m_embstr.data());
                m_embstr_len = raw.length;
                m_is_embstr = true;
                m_encoding = Encoding::EMBSTR;
                std::destroy_at(&raw);
            } else {
                auto new_ptr = std::make_unique<char[]>(raw.length);
                std::copy_n(raw.ptr.get(), raw.length, new_ptr.get());
                raw.ptr = std::move(new_ptr);
                raw.capacity = raw.length;
            }
        }
    }

    void FHString::clear() noexcept {
        if (!m_is_embstr && m_encoding == Encoding::RAW) {
            std::destroy_at(&m_data.raw_str_val);
        }

        m_encoding = Encoding::RAW;
        m_is_embstr = false;
        m_embstr_len = 0;
    }

    void FHString::convert_to_raw() {
        if (m_encoding == Encoding::RAW) {
            return;
        }

        size_t len = size();
        size_t new_capacity = std::bit_ceil(len * 3 / 2);
        auto ptr = std::make_unique<char[]>(new_capacity);
        if (m_is_embstr) {
            std::copy_n(m_embstr.data(), len, ptr.get());
            m_is_embstr = false;
            m_embstr_len = 0;
        } else {
            switch (m_encoding) {
                case Encoding::INT: {
                    auto s = to_chars_string(m_data.int_val);
                    std::copy_n(s.data(), s.size(), ptr.get());
                    len = s.size();
                    break;
                }
                default:
                    break;
            }
        }

        if (m_encoding == Encoding::RAW) {
            std::destroy_at(&m_data.raw_str_val);
        }

        std::construct_at(&m_data.raw_str_val, std::move(ptr), new_capacity, len);
        m_encoding = Encoding::RAW;
        m_is_embstr = false;
    }

    void FHString::ensure_capacity(size_t required) {
        if (m_is_embstr && required <= EMBSTR_LIMIT) {
            return;
        }

        convert_to_raw();
        RawString& raw = m_data.raw_str_val;
        if (required <= raw.capacity) {
            return;
        }

        size_t base = raw.capacity ? raw.capacity : 1;
        size_t new_cap = base + base / 2;
        if (new_cap < required) {
            new_cap = required;
        }

        new_cap = std::bit_ceil(new_cap);
        auto new_ptr = std::make_unique<char[]>(new_cap);
        std::copy_n(raw.ptr.get(), raw.length, new_ptr.get());

        raw.ptr = std::move(new_ptr);
        raw.capacity = new_cap;
    }

    void FHString::materialize() {
        if (m_encoding == Encoding::INT) {
            convert_to_raw();
        }
    }

    unsigned char* FHString::byte_ptr(size_t byte_offset) {
        if (byte_offset >= size()) {
            return nullptr;
        }

        return reinterpret_cast<unsigned char*>(data()) + byte_offset;
    }

    const unsigned char* FHString::byte_ptr(size_t byte_offset) const {
        if (byte_offset >= size()) {
            return nullptr;
        }

        return reinterpret_cast<const unsigned char*>(data()) + byte_offset;
    }

    std::span<unsigned char> FHString::byte_span() { return get_bytes(); }

    std::span<const unsigned char> FHString::byte_span() const { return get_bytes(); }

    std::optional<int64_t> FHString::from_chars_span() const noexcept {
        if (m_encoding == Encoding::INT) {
            return m_data.int_val;
        }

        auto bytes = byte_span();
        if (bytes.empty()) {
            return std::nullopt;
        }

        std::string_view sv{reinterpret_cast<const char*>(bytes.data()), bytes.size()};
        return parse_int(sv.data(), sv.size());
    }

    std::span<unsigned char> FHString::get_bytes() {
        switch (m_encoding) {
            case Encoding::EMBSTR:
                return {reinterpret_cast<unsigned char*>(m_embstr.data()), m_embstr_len};
            case Encoding::RAW: {
                const RawString& raw = m_data.raw_str_val;
                return {reinterpret_cast<unsigned char*>(raw.ptr.get()), raw.length};
            }
            case Encoding::INT: {
                int64_t value = m_data.int_val;
                auto [ptr, ec] =
                    std::to_chars(m_tmp_buf.data(), m_tmp_buf.data() + m_tmp_buf.size(), value);

                if (ec != std::errc{}) {
                    throw std::runtime_error("to_chars failed");
                }

                m_tmp_buf_len = ptr - m_tmp_buf.data();
                return {reinterpret_cast<unsigned char*>(m_tmp_buf.data()), m_tmp_buf_len};
            }
            default:
                return {};
        }
    }

    std::span<const unsigned char> FHString::get_bytes() const {
        return const_cast<FHString*>(this)->get_bytes();
    }

    size_t FHString::normalize_index(ssize_t idx, size_t length) const noexcept {
        ssize_t len = static_cast<ssize_t>(length);
        idx = (idx < 0) ? idx + len : idx;
        idx = std::clamp(idx, ssize_t{0}, len);
        return static_cast<size_t>(idx);
    }

    std::optional<int64_t> FHString::parse_int(const char* s, size_t len) const {
        if (len == 0) {
            return std::nullopt;
        }

        size_t i = 0;
        bool negative = false;
        if (s[i] == '-') {
            negative = true;
            ++i;
        } else if (s[i] == '+') {
            ++i;
        }

        if (i == len) {
            return std::nullopt;
        }

        std::optional<int64_t> result = int64_t{0};
        for (; i < len; ++i) {
            char c = s[i];
            if (!std::isdigit(static_cast<unsigned char>(c))) {
                return std::nullopt;
            }

            int64_t digit = c - '0';
            result = result.and_then([&](int64_t r) -> std::optional<int64_t> {
                if (negative) {
                    if (r < (std::numeric_limits<int64_t>::min() + digit) / 10) {
                        return std::nullopt;
                    }
                    return r * 10 - digit;
                } else {
                    if (r > (std::numeric_limits<int64_t>::max() - digit) / 10) {
                        return std::nullopt;
                    }
                    return r * 10 + digit;
                }
            });

            if (!result) {
                return std::nullopt;
            }
        }

        return result;
    }

    std::optional<long double> FHString::parse_ld(const char* s, size_t len) const {
        if (len == 0) {
            return std::nullopt;
        }

        size_t i = 0;
        bool negative = false;
        if (s[i] == '-') {
            negative = true;
            ++i;
        } else if (s[i] == '+') {
            ++i;
        }

        if (i == len) {
            return std::nullopt;
        }

        bool has_digits = false;
        unsigned __int128 int_part = 0;
        while (i < len && std::isdigit(static_cast<unsigned char>(s[i]))) {
            has_digits = true;
            int_part = int_part * 10 + (s[i++] - '0');
        }

        unsigned __int128 frac_part = 0;
        size_t frac_len = 0;
        if (i < len && s[i] == '.') {
            ++i;
            while (i < len && std::isdigit(static_cast<unsigned char>(s[i]))) {
                has_digits = true;
                frac_part = frac_part * 10 + (s[i++] - '0');
                ++frac_len;
            }
        }
        if (!has_digits) {
            return std::nullopt;
        }

        auto make_result = [&](unsigned __int128 ip, unsigned __int128 fp, size_t fl) {
            long double result = static_cast<long double>(ip);
            if (fl > 0) {
                static constexpr std::array<long double, 20> pow10_table = {
                    1e0L,  1e1L,  1e2L,  1e3L,  1e4L,  1e5L,  1e6L,  1e7L,  1e8L,  1e9L,
                    1e10L, 1e11L, 1e12L, 1e13L, 1e14L, 1e15L, 1e16L, 1e17L, 1e18L, 1e19L};

                result += static_cast<long double>(fp) / pow10_table[fl];
            }
            return result;
        };

        std::optional<long double> result = make_result(int_part, frac_part, frac_len);
        if (i < len && (s[i] == 'e' || s[i] == 'E')) {
            ++i;
            bool exp_negative = false;
            if (i < len && (s[i] == '+' || s[i] == '-')) {
                exp_negative = (s[i] == '-');
                ++i;
            }
            if (i == len || !std::isdigit(static_cast<unsigned char>(s[i]))) {
                return std::nullopt;
            }

            int exp_val = 0;
            while (i < len && std::isdigit(static_cast<unsigned char>(s[i]))) {
                exp_val = exp_val * 10 + (s[i++] - '0');
                if (exp_val > 10000) {
                    exp_val = 10000;
                }
            }

            result = result.and_then([&](long double r) -> std::optional<long double> {
                long double pow10 = 1.0L;
                long double base = 10.0L;
                int e = exp_val;
                while (e > 0) {
                    if (e & 1) {
                        pow10 *= base;
                    }
                    base *= base;
                    e >>= 1;
                }
                return exp_negative ? r / pow10 : r * pow10;
            });
        }

        return result.and_then([&](long double r) -> std::optional<long double> {
            if (i != len) {
                return std::nullopt;
            }
            if (negative) {
                r = -r;
            }
            if (std::isnan(r) || std::isinf(r)) {
                return std::nullopt;
            }
            return r;
        });
    }

    long double FHString::increment_float(long double inc) {
        long double current = 0.0L;
        if (!empty()) {
            auto current_opt = parse_ld(data(), size());
            if (!current_opt) {
                throw std::runtime_error("Value is not a valid float");
            }
            current = current_opt.value();
        }

        long double result = current + inc;
        if (std::isnan(result) || std::isinf(result)) {
            throw std::runtime_error("Resulting float is invalid (NaN or Inf)");
        }

        std::array<char, 128> buf{};
        auto [ptr, ec] = std::to_chars(
            buf.data(), buf.data() + buf.size(), result, std::chars_format::general, 17);
        if (ec != std::errc{}) {
            throw std::runtime_error("float to_chars failed");
        }

        std::string_view sv(buf.data(), ptr - buf.data());
        if (auto dot_pos = sv.find('.'); dot_pos != std::string_view::npos) {
            auto last_nonzero = sv.find_last_not_of('0');
            if (last_nonzero != std::string_view::npos && last_nonzero > dot_pos) {
                sv.remove_suffix(sv.size() - (last_nonzero + 1));
            }
            if (!sv.empty() && sv.back() == '.') {
                sv.remove_suffix(1);
            }
        }

        *this = FHString(sv);
        return result;
    }
}  // namespace fasthash
