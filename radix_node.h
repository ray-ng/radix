#pragma once

#include <algorithm>
#include <functional>
#include <map>

#include "slice.h"

namespace radix {

template <typename V>
class radix_tree;

template <typename V>
class radix_tree_iter;

template <typename V>
class radix_tree_node {
  friend class radix_tree<V>;
  friend class radix_tree_iter<V>;

  typedef typename std::map<Slice, radix_tree_node *>::iterator it_child;

 public:
  radix_tree_node();
  radix_tree_node(const Slice &key, V &value);

  void swap(radix_tree_node &);

 private:
  radix_tree_node(const radix_tree_node &);             // delete
  radix_tree_node &operator=(const radix_tree_node &);  // delete
  ~radix_tree_node();

  radix_tree_node *m_first = nullptr;
  radix_tree_node *m_last = nullptr;
  Slice m_key;
  union {
    struct {
      std::map<Slice, radix_tree_node *> m_children;
      std::vector<V> *m_heap;
    };
    std::vector<V> *m_value;
  };
  int m_count = 0;
};

template <typename V>
radix_tree_node<V>::radix_tree_node() {
  new (&m_children) std::map<Slice, radix_tree_node *>;
  m_heap = nullptr;
}

template <typename V>
radix_tree_node<V>::radix_tree_node(const Slice &key, V &value)
    : m_value(new std::vector<V>{value}), m_key(key) {}

template <typename V>
radix_tree_node<V>::~radix_tree_node() {
  if (m_count == 0 && !m_key.empty()) {
    delete m_value;
  } else {
    delete m_heap;
    it_child it;
    for (it = m_children.begin(); it != m_children.end(); ++it) {
      delete it->second;
    }
    m_children.~map();
  }
}

template <typename V>
void radix_tree_node<V>::swap(radix_tree_node<V> &other) {
  m_count = other.m_count;
  std::swap(m_first, other.m_first);
  std::swap(m_last, other.m_last);
  m_key.swap(other.m_key);
  m_children.swap(other.m_children);
  std::swap(m_heap, other.m_heap);
}

template <typename V>
class radix_tree_iter {
 public:
  radix_tree_iter(const radix_tree_node<V> *const begin, const radix_tree_node<V> *const end, int count,
                  bool order = true)
      : m_begin(begin)
      , m_end(end)
      , m_current(begin)
      , m_index(0)
      , m_cursor(0)
      , m_count(count)
      , m_order(order) {}
  radix_tree_iter() = default;
  radix_tree_iter(const radix_tree_iter &) = default;
  ~radix_tree_iter() = default;

  radix_tree_iter &operator=(const radix_tree_iter &iter) {
    m_begin = iter.m_current;
    m_end = iter.m_end;
    m_count = iter.m_count;
    m_current = m_begin;
    m_index = 0;
    m_cursor = 0;
    return *this;
  }

 private:
  const radix_tree_node<V> *m_begin = nullptr;
  const radix_tree_node<V> *m_end = nullptr;
  const radix_tree_node<V> *m_current = nullptr;
  std::size_t m_index = 0;
  int m_cursor = 0;
  bool m_order = true;
  int m_count = 0;

 public:
  int count() {
    return m_count;
  }

  void reset(int start, int count) {
    m_cursor = 0;
    m_count = count;
    for (int skip = 0; skip < start; ++skip) {
      if (m_begin != nullptr && m_begin != m_end && m_begin->m_last != nullptr) {
        m_begin = m_begin->m_last;
      }
    }
    m_current = m_begin;
  }

  bool valid() const {
    if (m_current == nullptr || m_current->m_value == nullptr) {
      return false;
    }
    if (m_cursor >= m_count) {
      return false;
    }
    if (m_cursor == m_count - 1 || m_current == m_end) {
      return m_index < m_current->m_value->size();
    }
    return true;
  }

  V value() const {
    return m_current->m_value->at(m_index);
  }

  void next() {
    ++m_index;
    if (m_index < m_current->m_value->size()) {
      return;
    }
    if (m_current != m_end && m_cursor < m_count) {
      m_index = 0;
      m_current = m_current->m_last;
      ++m_cursor;
    }
  }
};

}  // namespace radix
