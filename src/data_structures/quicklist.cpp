#include "data_structures/quicklist.hpp"

QuickList::QuickList(size_t node_cap) : m_node_cap(node_cap) {}
QuickList::~QuickList() { free_list(); }
