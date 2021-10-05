#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <list>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "radix_node.h"

namespace radix {

template <typename V>
class radix_tree {
 public:
  typedef std::size_t size_type;

  radix_tree()
      : m_size(0),
        m_first(nullptr),
        m_last(nullptr),
        m_root(new radix_tree_node<V>()) {}
  ~radix_tree() {
    radix_tree_node<V>* curr = m_root->m_first;
    while (curr != nullptr) {
      radix_tree_node<V>* temp = curr->m_last;
      delete curr;
      curr = temp;
    }
    delete m_root;
  }

  size_type size() const { return m_size; }
  bool empty() const { return m_size == 0; }
  void clear() {
    radix_tree_node<V>* curr = m_root->m_first;
    while (curr != nullptr) {
      radix_tree_node<V>* temp = curr->m_last;
      delete curr;
      curr = temp;
    }
    delete m_root;
    m_root = new radix_tree_node<V>();
    m_size = 0;
  }

  bool UTF8Decode(const char* str,
                  size_t len,
                  std::vector<Slice>& uchars) const;

  void insert(const std::string& pattern, V value);
  void match(const std::string& key, std::vector<V>& vec) const;
  void match(const std::string& key,
             std::vector<V>& vec,
             std::function<bool(V, V)> compfunc,
             int recall_limit) const;
  radix_tree_iter<V> match(const std::string& key) const;
  static void heap_insert(std::vector<V>* result,
                          V item,
                          std::function<bool(V, V)> compfunc,
                          int recall_limit);
  void finish(std::function<bool(V, V)> compfunc, int recall_limit) const;

 private:
  static const int MAX_NODES = 2000000;
  static const int SPLIT_NUMS = 3;

  std::list<std::string> m_patterns;
  size_type m_size;
  radix_tree_node<V>* m_root;
  radix_tree_node<V>* m_first;
  radix_tree_node<V>* m_last;

  radix_tree(const radix_tree& other);            // delete
  radix_tree& operator=(const radix_tree other);  // delete

  bool SliceDecode(const Slice& str, Slice* uchar) const;

  std::tuple<radix_tree_node<V>*, int, int> find_node(
      const std::vector<Slice>& key) const;
  void update_node(const std::vector<Slice>& key,
                   radix_tree_node<V>* old_last,
                   radix_tree_node<V>* new_last);
};

extern template class radix_tree<int>;

}  // namespace radix
