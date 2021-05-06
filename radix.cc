#include "radix.h"

namespace radix {

inline const Slice radix_substr(const Slice &key, int begin, int num) {
  if (begin < 0 || num < 0 || begin + num > key.size()) {
    return Slice();
  }
  return Slice(key.data() + begin, num);
}

inline const Slice radix_join(const Slice &key1, const Slice &key2) {
  if (key1.data() + key1.size() == key2.data()) {
    return Slice(key1.data(), key1.size() + key2.size());
  } else if (key2.data() + key2.size() == key1.data()) {
    return Slice(key2.data(), key2.size() + key1.size());
  } else {
    return Slice();
  }
}

inline int radix_length(const Slice &key) {
  return key.size();
}

template <typename V>
bool radix_tree<V>::UTF8Decode(const char *key, size_t len, std::vector<Slice> &uchars) const {
  char c;
  int code_len;

  uchars.clear();
  for (int i = 0; i < len;) {
    if (!key[i]) {
      break;
    }

    if ((key[i] & 0x80) == 0) {
      // ASCII char
      uchars.push_back(Slice(key + i, 1));
      i++;
      continue;
    }

    c = key[i];
    for (code_len = 1; code_len <= 6; code_len++) {
      c <<= 1;
      if (c & 0x80) {
        if ((key[i + code_len] & 0xC0) != 0x80) {
          return false;
        }
      } else {
        break;
      }
    }

    if (code_len == 1) {
      // illegal
      return false;
    }
    uchars.push_back(Slice(key + i, code_len));
    i += code_len;
  }
  return true;
}

template <typename V>
bool radix_tree<V>::SliceDecode(const Slice &key, Slice *uchar) const {
  const char *str = key.data();
  size_t len = key.size();
  char c;
  int code_len;
  if (uchar == nullptr) {
    return false;
  }

  for (int i = 0; i < len;) {
    if (!str[i]) {
      break;
    }

    if ((str[i] & 0x80) == 0) {
      // ASCII char
      *uchar = Slice(str + i, 1);
      return true;
    }

    c = str[i];
    for (code_len = 1; code_len <= 6; code_len++) {
      c <<= 1;
      if (c & 0x80) {
        if ((str[i + code_len] & 0xC0) != 0x80) {
          return false;
        }
      } else {
        break;
      }
    }

    if (code_len == 1) {
      // illegal
      return false;
    }
    *uchar = Slice(str + i, code_len);
    return true;
  }
  return false;
}

static const Slice CHILD = Slice("__CHILD");

template <typename V>
void radix_tree<V>::insert(const std::string &pattern, V value) {
  if (pattern.empty()) {
    return;
  }

  std::vector<Slice> uchars;
  if (!UTF8Decode(pattern.c_str(), pattern.length(), uchars) || uchars.empty()) {
    return;
  }
  Slice insert_key(pattern);

  std::tuple<radix_tree_node<V> *, int, int> node_depth = find_node(uchars);
  radix_tree_node<V> *match_node = std::get<0>(node_depth);
  int match_count = std::get<1>(node_depth);
  int match_depth = std::get<2>(node_depth);

  if (match_depth == uchars.size() && match_count == match_node->m_key.size()) {
    if (match_node->m_first != nullptr && match_node->m_first->m_value != nullptr) {
      match_node->m_first->m_value->emplace_back(value);
      // update_node(uchars, match_node->m_last, match_node->m_last);
      return;
    }
  }

  if (match_count < match_node->m_key.size()) {
    Slice new_key;
    if (SliceDecode(radix_substr(match_node->m_key, match_count, match_node->m_key.size() - match_count),
                    &new_key)) {
      radix_tree_node<V> *new_leaf = new radix_tree_node<V>(CHILD, value);
      radix_tree_node<V> *new_node = new radix_tree_node<V>();
      new_node->swap(*match_node);
      match_node->m_key = radix_substr(new_node->m_key, 0, match_count);
      new_node->m_key.remove_prefix(match_count);
      new_leaf->m_key = match_node->m_key;

      radix_tree_node<V> *temp_last = new_node->m_last;
      if (temp_last->m_last != nullptr) {
        temp_last->m_last->m_first = new_leaf;
        new_leaf->m_last = temp_last->m_last;
      }
      new_leaf->m_first = temp_last;
      temp_last->m_last = new_leaf;

      match_node->m_first = new_node->m_first;
      match_node->m_last = new_leaf;
      match_node->m_children[new_key] = new_node;

      if (match_depth != uchars.size()) {
        radix_tree_node<V> *new_node1 = new radix_tree_node<V>();
        int total_count = 0;
        for (int i = 0; i < match_depth; ++i) {
          total_count += uchars[i].size();
        }
        m_patterns.push_back(
            radix_substr(insert_key, total_count, insert_key.size() - total_count).ToString());
        Slice leaf_key = Slice(m_patterns.back());
        new_node1->m_key = leaf_key;
        new_node1->m_first = new_leaf;
        new_node1->m_last = new_leaf;
        new_leaf->m_key = leaf_key;
        match_node->m_children[radix_substr(leaf_key, 0, uchars[match_depth].size())] = new_node1;
      }

      update_node(uchars, temp_last, new_leaf);
    }
  } else if (match_count == match_node->m_key.size()) {
    radix_tree_node<V> *new_leaf = new radix_tree_node<V>(CHILD, value);
    new_leaf->m_key = match_node->m_key;

    if (match_depth != uchars.size()) {
      radix_tree_node<V> *new_node1 = new radix_tree_node<V>();
      int total_count = 0;
      for (int i = 0; i < match_depth; ++i) {
        total_count += uchars[i].size();
      }
      m_patterns.push_back(radix_substr(insert_key, total_count, insert_key.size() - total_count).ToString());
      Slice leaf_key = Slice(m_patterns.back());
      new_node1->m_key = leaf_key;
      new_node1->m_first = new_leaf;
      new_node1->m_last = new_leaf;
      new_leaf->m_key = leaf_key;
      match_node->m_children[radix_substr(leaf_key, 0, uchars[match_depth].size())] = new_node1;
    }

    radix_tree_node<V> *temp_last = match_node->m_last;
    if (temp_last == nullptr) {
      match_node->m_first = new_leaf;
      match_node->m_last = new_leaf;
    } else {
      if (temp_last->m_last != nullptr) {
        temp_last->m_last->m_first = new_leaf;
        new_leaf->m_last = temp_last->m_last;
      }
      new_leaf->m_first = temp_last;
      temp_last->m_last = new_leaf;
    }

    update_node(uchars, temp_last, new_leaf);
  }
}

template <typename V>
std::tuple<radix_tree_node<V> *, int, int> radix_tree<V>::find_node(const std::vector<Slice> &key) const {
  int count = 0, depth = 0;
  radix_tree_node<V> *result = m_root;
  if (key.empty()) {
    return {result, count, depth};
  }

  int len_key = key.size() - depth;
  while (len_key > 0) {
    typename radix_tree_node<V>::it_child it = result->m_children.find(key[depth]);
    if (it == result->m_children.end()) {
      break;
    }

    result = it->second;
    for (count = 0; count < result->m_key.size() && depth < key.size(); ++depth) {
      int m_key_len = result->m_key.size() - count;
      if (m_key_len < key[depth].size() ||
          key[depth] != radix_substr(result->m_key, count, key[depth].size())) {
        break;
      }
      count += key[depth].size();
    }
    if (count < result->m_key.size()) {
      break;
    }
    len_key = key.size() - depth;
  }

  return {result, count, depth};
}

template <typename V>
void radix_tree<V>::update_node(const std::vector<Slice> &key, radix_tree_node<V> *old_last,
                                radix_tree_node<V> *new_last) {
  int count = 0, depth = 0;
  radix_tree_node<V> *result = m_root;
  if (key.empty()) {
    return;
  }

  int len_key = key.size() - depth;
  while (len_key > 0) {
    typename radix_tree_node<V>::it_child it = result->m_children.find(key[depth]);
    if (it == result->m_children.end()) {
      break;
    }

    ++result->m_count;
    if (old_last == result->m_last) {
      result->m_last = new_last;
    }

    result = it->second;
    for (count = 0; count < result->m_key.size() && depth < key.size(); ++depth) {
      int m_key_len = result->m_key.size() - count;
      if (m_key_len < key[depth].size() ||
          key[depth] != radix_substr(result->m_key, count, key[depth].size())) {
        break;
      }
      count += key[depth].size();
    }
    if (count < result->m_key.size()) {
      break;
    }
    len_key = key.size() - depth;
  }

  ++result->m_count;
  if (old_last == result->m_last) {
    result->m_last = new_last;
  }
}

template <typename V>
void radix_tree<V>::match(const std::string &key, std::vector<V> &vec) const {
  std::vector<Slice> uchars;
  if (!UTF8Decode(key.data(), key.size(), uchars) || uchars.empty()) {
    return;
  }

  std::tuple<radix_tree_node<V> *, int, int> node_depth = find_node(uchars);
  radix_tree_node<V> *match_node = std::get<0>(node_depth);
  int match_count = std::get<1>(node_depth);
  int match_depth = std::get<2>(node_depth);

  if (match_depth == uchars.size()) {
    radix_tree_node<V> *temp = match_node->m_first;
    while (temp != nullptr) {
      if (temp->m_value != nullptr) {
        for (V p : *temp->m_value) {
          vec.push_back(p);
        }
      }
      if (temp == match_node->m_last) {
        break;
      }
      temp = temp->m_last;
    }
  }
}

template <typename V>
void radix_tree<V>::match(const std::string &key, std::vector<V> &vec, std::function<bool(V, V)> compfunc,
                          int recall_limit) const {
  std::vector<Slice> uchars;
  if (!UTF8Decode(key.data(), key.size(), uchars) || uchars.empty()) {
    return;
  }

  std::tuple<radix_tree_node<V> *, int, int> node_depth = find_node(uchars);
  radix_tree_node<V> *match_node = std::get<0>(node_depth);
  int match_count = std::get<1>(node_depth);
  int match_depth = std::get<2>(node_depth);

  if (match_depth == uchars.size()) {
    if (match_node->m_heap != nullptr) {
      int recall_num = recall_limit < match_node->m_heap->size() ? recall_limit : match_node->m_heap->size();
      vec.reserve(recall_num);
      for (int i = 0; i < recall_num; ++i) {
        vec.push_back(match_node->m_heap->at(i));
      }
    } else {
      std::unordered_set<V> item_set;
      radix_tree_node<V> *temp = match_node->m_first;
      while (temp != nullptr) {
        if (temp->m_value != nullptr) {
          for (V p : *temp->m_value) {
            if (item_set.count(p) > 0) {
              continue;
            }
            item_set.insert(p);
            heap_insert(&vec, p, compfunc, recall_limit);
          }
        }
        if (temp == match_node->m_last) {
          break;
        }
        temp = temp->m_last;
      }
      std::sort_heap(vec.begin(), vec.end(), compfunc);
    }
  }
}

template <typename V>
radix_tree_iter<V> radix_tree<V>::match(const std::string &key) const {
  std::vector<Slice> uchars;
  if (!UTF8Decode(key.data(), key.size(), uchars) || uchars.empty()) {
    return {nullptr, nullptr, 0};
  }

  std::tuple<radix_tree_node<V> *, int, int> node_depth = find_node(uchars);
  radix_tree_node<V> *match_node = std::get<0>(node_depth);
  int match_count = std::get<1>(node_depth);
  int match_depth = std::get<2>(node_depth);

  if (match_depth == uchars.size()) {
    return {match_node->m_first, match_node->m_last, match_node->m_count};
  }

  return {nullptr, nullptr, 0};
}

static const int nodes_threshold = 200;

template <typename V>
void radix_tree<V>::heap_insert(std::vector<V> *result, V item, std::function<bool(V, V)> compfunc,
                                int recall_limit) {
  if (result == nullptr) {
    return;
  }
  if (result->size() < recall_limit) {
    result->push_back(item);
    std::push_heap(result->begin(), result->end(), compfunc);
  } else if (compfunc(item, result->at(0))) {
    std::pop_heap(result->begin(), result->end(), compfunc);
    result->pop_back();
    result->push_back(item);
    std::push_heap(result->begin(), result->end(), compfunc);
  }
}

template <typename V>
void radix_tree<V>::finish(std::function<bool(V, V)> compfunc, int recall_limit) const {
  if (m_root->m_count < nodes_threshold) return;

  std::vector<radix_tree_node<V> *> process_nodes;
  process_nodes.push_back(m_root);
  int index = 0;

  while (index < process_nodes.size()) {
    radix_tree_node<V> *current = process_nodes[index];
    for (typename radix_tree_node<V>::it_child iter = current->m_children.begin();
         iter != current->m_children.end(); ++iter) {
      if (iter->second->m_count > nodes_threshold) {
        process_nodes.push_back(iter->second);
      }
    }
    ++index;
  }

  for (int index = process_nodes.size() - 1; index >= 0; --index) {
    radix_tree_node<V> *current = process_nodes[index];
    current->m_heap = new std::vector<V>();
    std::unordered_set<V> item_set;
    std::vector<std::pair<radix_tree_node<V> *, radix_tree_node<V> *>> heap_range;
    if (!current->m_children.empty()) {
      heap_range.reserve(current->m_children.size());
    }

    for (typename radix_tree_node<V>::it_child iter = current->m_children.begin();
         iter != current->m_children.end(); ++iter) {
      if (iter->second->m_heap != nullptr) {
        for (V item : *iter->second->m_heap) {
          if (item_set.count(item) > 0) {
            continue;
          }
          item_set.insert(item);
          heap_insert(current->m_heap, item, compfunc, recall_limit);
        }
        heap_range.emplace_back(iter->second->m_first, iter->second->m_last);
      }
    }

    int range_index = 0;
    radix_tree_node<V> *temp = current->m_first;
    bool skip_range = false;
    while (temp != nullptr) {
      if (range_index < heap_range.size()) {
        if (!skip_range && temp == heap_range[range_index].first) {
          skip_range = true;
        }
      }
      if (!skip_range) {
        if (temp->m_value != nullptr) {
          for (V p : *temp->m_value) {
            if (item_set.count(p) > 0) {
              continue;
            }
            item_set.insert(p);
            heap_insert(current->m_heap, p, compfunc, recall_limit);
          }
        }
        if (temp == current->m_last) {
          break;
        }
      }
      if (range_index < heap_range.size()) {
        if (skip_range && temp == heap_range[range_index].second) {
          skip_range = false;
          ++range_index;
        }
      }
      temp = temp->m_last;
    }

    std::sort_heap(current->m_heap->begin(), current->m_heap->end(), compfunc);
  }
}

template class radix_tree<int>;

}  // namespace radix
