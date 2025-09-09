#include "data_structures/quicklist.hpp"

namespace fasthash {
    QuickList::QuickList(size_t node_cap) : m_node_cap(node_cap) {}

    QuickList::~QuickList() { free_list(); }

    QuickList::QuickList(const QuickList& other) { copy_from(other); }

    QuickList& QuickList::operator=(const QuickList& other) {
        if (this != &other) {
            free_list();
            copy_from(other);
        }
        return *this;
    }

    QuickList::QuickList(QuickList&& other) noexcept :
        m_head(other.m_head), m_tail(other.m_tail), m_size(other.m_size),
        m_node_cap(other.m_node_cap) {
        other.m_head = nullptr;
        other.m_tail = nullptr;
        other.m_size = 0;
    }

    QuickList& QuickList::operator=(QuickList&& other) noexcept {
        if (this != &other) {
            free_list();
            m_head = other.m_head;
            m_tail = other.m_tail;
            m_size = other.m_size;
            m_node_cap = other.m_node_cap;

            other.m_head = nullptr;
            other.m_tail = nullptr;
            other.m_size = 0;
        }
        return *this;
    }

    void QuickList::lpush(const FHString& value) {
        Node* head = ensure_head();
        if (head->values.size() >= m_node_cap) {
            Node* n = new Node(m_node_cap);
            n->next = m_head;
            m_head->prev = n;
            m_head = n;
            head = n;
        }

        head->values.insert(head->values.begin(), value);
        ++m_size;
    }
}  // namespace fasthash
