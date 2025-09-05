#pragma once

#include <cstddef>
#include <vector>
#include "data_structures/string.hpp"

class QuickList {
public:
    struct Node {
        std::vector<FastHashString> values;
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

    void lpush(const FastHashString& value);
    void rpush(const FastHashString& value);

    std::optional<FastHashString> lpop();
    std::optional<FastHashString> rpop();
    size_t llen() const noexcept;

    void lmove(QuickList& dst, bool from_left, bool to_left);
    std::vector<FastHashString> lrange(size_t start, size_t end) const;
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
