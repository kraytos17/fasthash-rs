#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "data_structures/string.hpp"

namespace fasthash {
    class QuickList {
    public:
        struct Node {
            std::vector<FHString> values;
            Node* prev{nullptr};
            Node* next{nullptr};

            explicit Node(size_t cap_hint = 16) { values.reserve(cap_hint); }
        };

        explicit QuickList(size_t node_cap = 16);
        ~QuickList();

        QuickList(const QuickList& other);
        QuickList& operator=(const QuickList& other);
        QuickList(QuickList&& other) noexcept;
        QuickList& operator=(QuickList&& other) noexcept;

        void lpush(const FHString& value);
        void rpush(const FHString& value);

        std::optional<FHString> lpop();
        std::optional<FHString> rpop();
        size_t llen() const noexcept;

        void lmove(QuickList& dst, bool from_left, bool to_left);
        std::vector<FHString> lrange(size_t start, size_t end) const;
        void ltrim(size_t start, size_t end);

    private:
        Node* m_head{nullptr};
        Node* m_tail{nullptr};
        size_t m_size{0};
        size_t m_node_cap{0};

        void free_list();
        void copy_from(const QuickList& other);
        Node* ensure_head();
        Node* ensure_tail();
    };
}  // namespace fasthash
