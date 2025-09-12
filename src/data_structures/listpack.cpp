#include "data_structures/listpack.hpp"

#include <array>
#include <cstdint>
#include <expected>
#include <limits>

namespace fasthash {
    namespace detail {
        template<typename T>
        constexpr T min() noexcept {
            return std::numeric_limits<T>::min();
        }

        template<typename T>
        constexpr T max() noexcept {
            return std::numeric_limits<T>::max();
        }

        std::expected<Listpack::Header, Listpack::Error> parse_header(
            std::span<const std::byte> buf) noexcept {
            if (buf.size() < 7) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            Listpack::Header h;
            h.tot_bytes = Listpack::read_u32_le(buf.data());
            h.num_elems = Listpack::read_u16_le(buf.data() + 4);
            if (h.tot_bytes != buf.size()) {
                return std::unexpected(Listpack::Error::BadHeader);
            }
            if (buf.back() != Listpack::Terminator) {
                return std::unexpected(Listpack::Error::BadHeader);
            }

            return h;
        }

        std::expected<std::pair<size_t, size_t>, Listpack::Error> read_backlen_rtl(
            const std::byte* right, const std::byte* left_bound) noexcept {
            size_t value = 0;
            size_t shift = 0;
            size_t count = 0;
            const std::byte* p = right;
            while (true) {
                if (p < left_bound) {
                    return std::unexpected(Listpack::Error::BadLength);
                }

                const uint8_t u = static_cast<uint8_t>(*p);
                const uint8_t chunk = u & 0x7F;
                value |= (size_t(chunk) << shift);
                ++count;

                const bool more_to_left = (u & 0x80) != 0;
                if (!more_to_left) {
                    break;
                }
                if (p == left_bound) {
                    return std::unexpected(Listpack::Error::BadLength);
                }

                --p;
                shift += 7;
                if (count > 10) {
                    return std::unexpected(Listpack::Error::BadLength);
                }
            }

            if (value == 0) {
                return std::unexpected(Listpack::Error::BadLength);
            }

            return std::make_pair(value, count);
        }

        void write_backlen_rtl(std::byte* end_ptr, size_t total_len) noexcept {
            std::array<std::byte, 10> tmp;
            int count = 0;
            size_t v = total_len;
            do {
                tmp[count++] = std::byte(v & 0x7F);
                v >>= 7;
            } while (v);

            for (int i = 0; i < count; ++i) {
                const bool more = (i != 0);
                end_ptr[-i] = std::byte(static_cast<uint8_t>(tmp[i]) | (more ? 0x80u : 0u));
            }
        }

        size_t backlen_size_for(size_t x) noexcept {
            size_t s = 1;
            while (x > ((size_t(1) << (7 * s)) - 1)) {
                ++s;
            }

            return s;
        }

        bool is_small_number_string(std::string_view s, uint8_t& out) noexcept {
            if (s.empty()) {
                return false;
            }

            uint32_t v = 0;
            for (char c: s) {
                if (c < '0' || c > '9') {
                    return false;
                }

                v = v * 10u + uint32_t(c - '0');
                if (v > 127u) {
                    return false;
                }
            }

            if (s.size() > 1 && s.front() == '0') {
                return false;
            }

            out = static_cast<uint8_t>(v);
            return true;
        }

        constexpr bool fits_i13(int64_t v) noexcept { return v >= -4096 && v <= 4095; }
        constexpr bool fits_i16(int64_t v) noexcept {
            return v >= min<int16_t>() && v <= max<int16_t>();
        }

        constexpr bool fits_i24(int64_t v) noexcept {
            return v >= -(1ll << 23) && v <= ((1ll << 23) - 1);
        }

        constexpr bool fits_i32(int64_t v) noexcept {
            return v >= min<int32_t>() && v <= max<int32_t>();
        }

        constexpr void write_le16(std::byte* p, int16_t v) noexcept {
            const uint16_t u = static_cast<uint16_t>(v);
            p[0] = std::byte(u & 0xFF);
            p[1] = std::byte((u >> 8) & 0xFF);
        }

        constexpr void write_le24(std::byte* p, int32_t v) noexcept {
            const uint32_t u = static_cast<uint32_t>(v);
            p[0] = std::byte(u & 0xFF);
            p[1] = std::byte((u >> 8) & 0xFF);
            p[2] = std::byte((u >> 16) & 0xFF);
        }

        constexpr void write_le32(std::byte* p, int32_t v) noexcept {
            const uint32_t u = static_cast<uint32_t>(v);
            p[0] = std::byte(u & 0xFF);
            p[1] = std::byte((u >> 8) & 0xFF);
            p[2] = std::byte((u >> 16) & 0xFF);
            p[3] = std::byte((u >> 24) & 0xFF);
        }

        constexpr void write_le64(std::byte* p, int64_t v) noexcept {
            const uint64_t u = static_cast<uint64_t>(v);
            p[0] = std::byte(u & 0xFF);
            p[1] = std::byte((u >> 8) & 0xFF);
            p[2] = std::byte((u >> 16) & 0xFF);
            p[3] = std::byte((u >> 24) & 0xFF);
            p[4] = std::byte((u >> 32) & 0xFF);
            p[5] = std::byte((u >> 40) & 0xFF);
            p[6] = std::byte((u >> 48) & 0xFF);
            p[7] = std::byte((u >> 56) & 0xFF);
        }

        constexpr void write_be32(std::byte* p, uint32_t v) noexcept {
            p[0] = std::byte((v >> 24) & 0xFF);
            p[1] = std::byte((v >> 16) & 0xFF);
            p[2] = std::byte((v >> 8) & 0xFF);
            p[3] = std::byte(v & 0xFF);
        }

        int string_encoding_and_hp_len(std::string_view s, size_t& hp_len,
                                       uint8_t& smallnum) noexcept {
            if (is_small_number_string(s, smallnum)) {
                hp_len = 1;
                return LP_ENCODING_7BIT_UINT;
            }

            const size_t n = s.size();
            if (n <= 63) {
                hp_len = LP_ENCODING_6BIT_STR_ENTRY_SIZE + n;
                return LP_ENCODING_STRING_6BIT;
            }
            if (n <= 4095) {
                hp_len = LP_ENCODING_12BIT_STR_ENTRY_SIZE + n;
                return LP_ENCODING_STRING_12BIT;
            }

            hp_len = LP_ENCODING_32BIT_STR_ENTRY_SIZE + n;
            return LP_ENCODING_STRING_32BIT;
        }

        size_t int_hp_len(int64_t v) noexcept {
            if (fits_i13(v)) {
                return 2;
            }
            if (fits_i16(v)) {
                return 1 + 2;
            }
            if (fits_i24(v)) {
                return 1 + 3;
            }
            if (fits_i32(v)) {
                return 1 + 4;
            }
            return 1 + 8;
        }

        size_t write_string_head_payload(std::byte* dst, std::string_view s) noexcept {
            const size_t len = s.size();
            if (len <= 63) {
                dst[0] = std::byte(0x80 | (static_cast<uint8_t>(len) & 0x3F));
                if (len) {
                    std::memcpy(dst + 1, s.data(), len);
                }
                return 1 + len;
            }
            if (len <= 4095) {
                uint16_t l = static_cast<uint16_t>(len);
                dst[0] = std::byte(0xE0 | ((l >> 8) & 0x0F));
                dst[1] = std::byte(l & 0xFF);
                std::memcpy(dst + 2, s.data(), len);
                return 2 + len;
            }

            dst[0] = std::byte(0xF0);
            write_be32(dst + 1, static_cast<uint32_t>(len));
            std::memcpy(dst + 5, s.data(), len);
            return 5 + len;
        }

        size_t write_int_head_payload(std::byte* dst, int64_t v) noexcept {
            if (v >= 0 && v <= 127) {
                dst[0] = std::byte(static_cast<uint8_t>(v & 0xFF));
                return 1;
            }
            if (fits_i13(v)) {
                uint16_t u = static_cast<uint16_t>(v & 0x1FFF);
                dst[0] = std::byte(0xC0 | ((u >> 8) & 0x1F));
                dst[1] = std::byte(u & 0xFF);
                return 2;
            }
            if (fits_i16(v)) {
                dst[0] = std::byte(0xF1);
                write_le16(dst + 1, static_cast<int16_t>(v));
                return 3;
            }
            if (fits_i24(v)) {
                dst[0] = std::byte(0xF2);
                write_le24(dst + 1, static_cast<int32_t>(v));
                return 4;
            }
            if (fits_i32(v)) {
                dst[0] = std::byte(0xF3);
                write_le32(dst + 1, static_cast<int32_t>(v));
                return 5;
            }

            dst[0] = std::byte(0xF4);
            write_le64(dst + 1, v);
            return 9;
        }

        std::expected<size_t, Listpack::Error> entry_total_len(const std::byte* entry,
                                                               const std::byte* left_bound,
                                                               const std::byte* term) noexcept {
            if (!entry || entry >= term) {
                return std::unexpected(Listpack::Error::BadLength);
            }

            const uint8_t first = static_cast<uint8_t>(*entry);
            size_t header_size = 1;
            size_t payload_size = 0;
            if ((first & 0x80) == 0) {
                header_size = 1;
                payload_size = 0;
            } else if ((first & 0xC0) == 0x80) {
                header_size = 1;
                payload_size = static_cast<size_t>(first & 0x3F);
            } else if ((first & 0xF0) == 0xE0) {
                if (entry + 2 > term) {
                    return std::unexpected(Listpack::Error::Truncated);
                }

                header_size = 2;
                payload_size =
                    (static_cast<uint32_t>(first & 0x0F) << 8) | static_cast<uint8_t>(entry[1]);
            } else if (first == 0xF0) {
                if (entry + 5 > term) {
                    return std::unexpected(Listpack::Error::Truncated);
                }

                header_size = 5;
                payload_size = (static_cast<uint32_t>(entry[1]) << 24) |
                               (static_cast<uint32_t>(entry[2]) << 16) |
                               (static_cast<uint32_t>(entry[3]) << 8) |
                               static_cast<uint32_t>(entry[4]);
            } else if (first >= 0xF1 && first <= 0xF4) {
                switch (first) {
                    case 0xF1:
                        header_size = 1 + 2;
                        payload_size = 0;
                        break;
                    case 0xF2:
                        header_size = 1 + 3;
                        payload_size = 0;
                        break;
                    case 0xF3:
                        header_size = 1 + 4;
                        payload_size = 0;
                        break;
                    case 0xF4:
                        header_size = 1 + 8;
                        payload_size = 0;
                        break;
                }
            } else if ((first & 0xE0) == 0xC0) {
                header_size = 2;
                payload_size = 0;
            } else {
                return std::unexpected(Listpack::Error::BadEncoding);
            }

            const std::byte* after_payload = entry + header_size + payload_size;
            if (after_payload >= term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            auto rb = read_backlen_rtl(after_payload, left_bound);
            if (!rb) {
                return std::unexpected(rb.error());
            }

            return rb->first;
        }

        std::expected<size_t, Listpack::Error> encode_elem(const Listpack::ElemValue& v,
                                                           std::byte* dst,
                                                           size_t capacity) noexcept {
            if (!dst) {
                return std::unexpected(Listpack::Error::OutOfRange);
            }

            size_t header_and_payload = 0;
            if (v.type == Listpack::ElemType::StrView) {
                std::string_view s{v.data.sv.ptr, v.data.sv.len};
                header_and_payload = write_string_head_payload(dst, s);
            } else {
                header_and_payload = write_int_head_payload(dst, v.data.i64);
            }

            size_t backlen_size = 1;
            while (true) {
                size_t candidate = backlen_size_for(header_and_payload + backlen_size);
                if (candidate == backlen_size) {
                    break;
                }

                backlen_size = candidate;
            }

            size_t total_size = header_and_payload + backlen_size;
            if (total_size > capacity) {
                return std::unexpected(Listpack::Error::OutOfRange);
            }

            write_backlen_rtl(dst + total_size - 1, total_size);
            return total_size;
        }
    }  // namespace detail

    std::expected<Listpack::Header, Listpack::Error> Listpack::View::header() const noexcept {
        return detail::parse_header(m_buffer);
    }

    std::expected<size_t, Listpack::Error> Listpack::View::size() const noexcept {
        auto h = detail::parse_header(m_buffer);
        if (!h) {
            return std::unexpected(h.error());
        }
        if (h->known()) {
            return static_cast<size_t>(h->num_elems);
        }
        if (m_buffer.size() < 7) {
            return std::unexpected(Error::Truncated);
        }

        const std::byte* base = m_buffer.data();
        const std::byte* left_bound = base + 6;
        const std::byte* term = base + (m_buffer.size() - 1);
        const std::byte* p = term - 1;

        size_t count = 0;
        if (p < left_bound) {
            if (m_buffer.size() != 7) {
                return std::unexpected(Error::BadLength);
            }
            return size_t{0};
        }

        while (p >= left_bound) {
            auto rb = detail::read_backlen_rtl(p, left_bound);
            if (!rb) {
                return std::unexpected(rb.error());
            }

            const size_t entry_total_len = rb->first;
            const size_t backlen_bytes = rb->second;
            if (entry_total_len <= backlen_bytes) {
                return std::unexpected(Error::BadLength);
            }

            const std::byte* entry_start = p - (entry_total_len - 1);
            if (entry_start < left_bound) {
                return std::unexpected(Error::BadLength);
            }

            ++count;
            if (entry_start == left_bound) {
                p = left_bound - 1;
                break;
            } else {
                p = entry_start - 1;
            }
        }

        if (p != left_bound - 1) {
            return std::unexpected(Error::BadLength);
        }
        return count;
    }

    Listpack::View::Iterator::Iterator(const std::byte* base, const std::byte* cur,
                                       const std::byte* left_bound, const std::byte* term)
      : m_base(base), m_cur(cur), m_left_bound(left_bound), m_term(term) {
        if (term && (term < base || term >= base + 3 || *term != Terminator)) {
            m_cur = nullptr;
            return;
        }
        if (m_cur && m_cur < m_term) {
            decode_current();
        }
    }

    std::expected<Listpack::ElemValue, Listpack::Error> decode_elem(
        const std::byte* entry, const std::byte* left_bound, const std::byte* term) noexcept {
        if (!entry || entry >= term) {
            return std::unexpected(Listpack::Error::Truncated);
        }

        const uint8_t first = static_cast<uint8_t>(*entry);
        size_t header_size = 0;
        size_t payload_size = 0;

        Listpack::ElemValue value{};
        if (detail::is_7bit_int_enc(first)) {
            int64_t v = static_cast<int8_t>(first);
            value = Listpack::ElemValue::make_int(v);
            header_size = 1;
            payload_size = 0;
        } else if (detail::is_6bit_str_enc(first)) {
            uint32_t slen = detail::len_6bit_str(first);
            header_size = 1;
            if (entry + header_size + slen > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            value = Listpack::ElemValue::make_str(reinterpret_cast<const char*>(entry + 1), slen);
            payload_size = slen;
        } else if (detail::is_13bit_int_enc(first)) {
            if (entry + 2 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            int16_t v = static_cast<int16_t>((first & 0x1F) << 8) | static_cast<uint8_t>(entry[1]);
            if (v & 0x1000) {
                v |= 0xE000;
            }

            value = Listpack::ElemValue::make_int(v);
            header_size = 2;
            payload_size = 0;
        } else if (detail::is_12bit_str_enc(first)) {
            if (entry + 2 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            uint32_t slen = detail::len_12bit_str(first, static_cast<uint8_t>(entry[1]));
            header_size = 2;
            if (entry + header_size + slen > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            value = Listpack::ElemValue::make_str(reinterpret_cast<const char*>(entry + 2), slen);
            payload_size = slen;
        } else if (detail::is_32bit_str_enc(first)) {
            if (entry + 5 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            uint32_t slen = detail::len_32bit_str(reinterpret_cast<const uint8_t*>(entry));
            header_size = 5;
            if (entry + header_size + slen > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            value = Listpack::ElemValue::make_str(reinterpret_cast<const char*>(entry + 5), slen);
            payload_size = slen;
        } else if (detail::is_int16_enc(first)) {
            if (entry + 3 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            int16_t v;
            std::memcpy(&v, entry + 1, 2);
            value = Listpack::ElemValue::make_int(v);
            header_size = 3;
            payload_size = 0;
        } else if (detail::is_int24_enc(first)) {
            if (entry + 4 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            int32_t v = 0;
            std::memcpy(&v, entry + 1, 3);
            if (v & 0x00800000) {
                v |= 0xFF000000;
            }

            value = Listpack::ElemValue::make_int(v);
            header_size = 4;
            payload_size = 0;
        } else if (detail::is_int32_enc(first)) {
            if (entry + 5 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            int32_t v;
            std::memcpy(&v, entry + 1, 4);
            value = Listpack::ElemValue::make_int(v);
            header_size = 5;
            payload_size = 0;
        } else if (detail::is_int64_enc(first)) {
            if (entry + 9 > term) {
                return std::unexpected(Listpack::Error::Truncated);
            }

            int64_t v;
            std::memcpy(&v, entry + 1, 8);
            value = Listpack::ElemValue::make_int(v);
            header_size = 9;
            payload_size = 0;
        } else {
            return std::unexpected(Listpack::Error::BadEncoding);
        }

        const std::byte* after_payload = entry + header_size + payload_size;
        if (after_payload >= term) {
            return std::unexpected(Listpack::Error::Truncated);
        }

        auto rb = detail::read_backlen_rtl(after_payload, left_bound);
        if (!rb) {
            return std::unexpected(rb.error());
        }

        const size_t declared_total = header_size + payload_size + rb->second;
        if (declared_total != rb->first) {
            return std::unexpected(Listpack::Error::BadLength);
        }

        return value;
    }

    void Listpack::View::Iterator::decode_current() noexcept {
        auto dec = decode_elem(m_cur, m_left_bound, m_term);
        if (dec.has_value()) {
            m_current = dec.value();
            m_err = Error::Ok;
        } else {
            m_current = ElemValue::make_int(0);
            m_err = dec.error();
        }
    }

    Listpack::View::Iterator& Listpack::View::Iterator::operator++() {
        if (has_error()) {
            return *this;
        }
        if (!m_cur || m_cur >= m_term) {
            m_err = Error::OutOfRange;
            return *this;
        }

        auto len = detail::entry_total_len(m_cur, m_left_bound, m_term);
        if (!len.has_value()) {
            m_err = len.error();
            m_cur = m_term;
            return *this;
        }

        m_cur += *len;
        if (m_cur < m_term) {
            decode_current();
        }

        return *this;
    }

    Listpack::View::Iterator Listpack::View::Iterator::operator++(int) {
        Iterator tmp = *this;
        ++(*this);
        return tmp;
    }

    Listpack::View::Iterator& Listpack::View::Iterator::operator--() {
        if (!m_cur || m_cur <= m_left_bound) {
            return *this;
        }

        auto rb = detail::read_backlen_rtl(m_cur - 1, m_left_bound);
        if (!rb) {
            m_cur = nullptr;
            return *this;
        }

        size_t entry_total_len = rb->first;
        if (entry_total_len == 0 || entry_total_len > static_cast<size_t>(m_cur - m_left_bound)) {
            m_cur = nullptr;
            return *this;
        }

        m_cur -= entry_total_len;
        decode_current();
        return *this;
    }

    Listpack::View::Iterator Listpack::View::Iterator::operator--(int) {
        Iterator tmp = *this;
        --(*this);
        return tmp;
    }

    Listpack::View::Iterator Listpack::View::begin() const noexcept {
        const std::byte* base = m_buffer.data();
        const std::byte* left_bound = base + 6;
        const std::byte* term = base + (m_buffer.size() - 1);
        if (m_buffer.size() < 7 || *term != Terminator || left_bound >= term) {
            return Iterator(base, nullptr, left_bound, term);
        }

        return Iterator(base, left_bound, left_bound, term);
    }

    Listpack::View::Iterator Listpack::View::end() const noexcept {
        const std::byte* base = m_buffer.data();
        const std::byte* left_bound = base + 6;
        const std::byte* term = base + (m_buffer.size() - 1);

        return Iterator(base, term, left_bound, term);
    }

    std::expected<Listpack::Header, Listpack::Error> Listpack::MutView::header() const noexcept {
        return detail::parse_header(std::span<const std::byte>(m_buffer.data(), m_buffer.size()));
    }

    std::expected<void, Listpack::Error> Listpack::MutView::update_in_place(
        size_t index, ElemValue newval) noexcept {
        const std::byte* base = m_buffer.data();
        const std::byte* left_bound = base + 6;
        const std::byte* term = base + (m_buffer.size() - 1);
        const std::byte* entry = left_bound;
        for (size_t i = 0; i < index; ++i) {
            auto len = detail::entry_total_len(entry, left_bound, term);
            if (!len) {
                return std::unexpected(len.error());
            }

            entry += *len;
            if (entry >= term) {
                return std::unexpected(Error::OutOfRange);
            }
        }

        auto orig_len = detail::entry_total_len(entry, left_bound, term);
        if (!orig_len) {
            return std::unexpected(orig_len.error());
        }

        std::array<std::byte, 1024> scratch;
        auto enc = detail::encode_elem(newval, scratch.data(), scratch.size());
        if (!enc) {
            return std::unexpected(enc.error());
        }
        if (enc.value() != orig_len.value()) {
            return std::unexpected(Error::SizeMismatch);
        }

        std::memcpy(const_cast<std::byte*>(entry), scratch.data(), *enc);
        return {};
    }

    std::expected<void, Listpack::Error> Listpack::Pack::push_back(ElemValue v) {
        auto h = detail::parse_header(m_buffer.span());
        if (!h) {
            return std::unexpected(h.error());
        }

        std::array<std::byte, 1024> scratch;
        auto enc = detail::encode_elem(v, scratch.data(), scratch.size());
        if (!enc) {
            return std::unexpected(enc.error());
        }

        size_t total_len = enc.value();
        size_t old_size = m_buffer.size;
        size_t insert_off = old_size - 1;

        m_buffer.resize(old_size + total_len);
        std::byte* base = m_buffer.get();

        std::memcpy(base + insert_off, scratch.data(), total_len);
        base[insert_off + total_len] = Terminator;
        size_t new_size = old_size + total_len;
        write_u32_le(base + 0, static_cast<uint32_t>(new_size));
        uint16_t ne = read_u16_le(base + 4);
        if (ne != UnknownCount) {
            uint32_t inc = static_cast<uint32_t>(ne) + 1u;
            if (inc >= UnknownCount) {
                write_u16_le(base + 4, UnknownCount);
            } else {
                write_u16_le(base + 4, static_cast<uint16_t>(inc));
            }
        }

        return {};
    }

    std::expected<void, Listpack::Error> Listpack::Pack::insert(size_t index, ElemValue v) {
        auto h = detail::parse_header(m_buffer.span());
        if (!h) {
            return std::unexpected(h.error());
        }

        size_t count;
        if (h->known()) {
            count = h->num_elems;
        } else {
            auto sz = View(m_buffer.span()).size();
            if (!sz) return std::unexpected(sz.error());
            count = *sz;
        }

        if (index > count) {
            return std::unexpected(Error::OutOfRange);
        }

        const std::byte* base = m_buffer.get();
        const std::byte* left_bound = base + HeaderSize;
        const std::byte* term = base + (m_buffer.size - 1);
        const std::byte* pos = left_bound;
        for (size_t i = 0; i < index; ++i) {
            auto len = detail::entry_total_len(pos, left_bound, term);
            if (!len) {
                return std::unexpected(len.error());
            }

            pos += len.value();
        }

        size_t prefix_len = pos - base;

        std::array<std::byte, 1024> scratch;
        auto enc = detail::encode_elem(v, scratch.data(), scratch.size());
        if (!enc) {
            return std::unexpected(enc.error());
        }

        size_t new_tot = h->tot_bytes + enc.value();
        Buffer newbuf(new_tot);

        std::memcpy(newbuf.get(), base, prefix_len);
        std::memcpy(newbuf.get() + prefix_len, scratch.data(), enc.value());

        size_t suffix_len = h->tot_bytes - prefix_len;
        std::memcpy(newbuf.get() + prefix_len + enc.value(), base + prefix_len, suffix_len);

        write_u32_le(newbuf.get() + 0, static_cast<uint32_t>(new_tot));
        uint16_t new_count =
            (h->num_elems == UnknownCount) ? UnknownCount : static_cast<uint16_t>(h->num_elems + 1);

        write_u16_le(newbuf.get() + 4, new_count);
        m_buffer = std::move(newbuf);

        return {};
    }

    std::expected<void, Listpack::Error> Listpack::Pack::erase(size_t index) {
        auto h = detail::parse_header(m_buffer.span());
        if (!h) {
            return std::unexpected(h.error());
        }

        size_t count;
        if (h->known()) {
            count = h->num_elems;
        } else {
            auto sz = View(m_buffer.span()).size();
            if (!sz) return std::unexpected(sz.error());
            count = *sz;
        }

        if (index >= count) {
            return std::unexpected(Error::OutOfRange);
        }

        const std::byte* base = m_buffer.get();
        const std::byte* left_bound = base + HeaderSize;
        const std::byte* term = base + (m_buffer.size - 1);
        const std::byte* entry = left_bound;
        for (size_t i = 0; i < index; ++i) {
            auto len = detail::entry_total_len(entry, left_bound, term);
            if (!len) {
                return std::unexpected(len.error());
            }
            entry += *len;
        }

        auto entry_len = detail::entry_total_len(entry, left_bound, term);
        if (!entry_len) {
            return std::unexpected(entry_len.error());
        }

        size_t prefix_len = entry - base;
        size_t suffix_off = prefix_len + *entry_len;
        size_t suffix_len = h->tot_bytes - suffix_off;
        size_t new_tot = h->tot_bytes - *entry_len;
        Buffer newbuf(new_tot);

        std::memcpy(newbuf.get(), base, prefix_len);
        std::memcpy(newbuf.get() + prefix_len, base + suffix_off, suffix_len);

        write_u32_le(newbuf.get() + 0, static_cast<uint32_t>(new_tot));
        uint16_t new_count =
            (h->num_elems == UnknownCount) ? UnknownCount : static_cast<uint16_t>(h->num_elems - 1);

        write_u16_le(newbuf.get() + 4, new_count);
        m_buffer = std::move(newbuf);
        return {};
    }
}  // namespace fasthash
