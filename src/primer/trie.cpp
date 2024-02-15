#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include "common/exception.h"

namespace bustub {

// Suppress the warning for the C++20 extension
#pragma clang diagnostic ignored "-Wc++20-extensions"
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!root_) {
    return nullptr;
  }
  auto p = root_;
  for (char ch : key) {
    if (!p->children_.count(ch)) {
      return nullptr;
    }
    p = p->children_.at(ch);
  }
  auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(p.get());
  if (!ret) {
    return nullptr;
  }
  return p->is_value_node_ ? ret->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> p;
  if (!root_) {
    p = std::make_shared<TrieNode>();
  } else {
    p = std::shared_ptr<TrieNode>(root_->Clone());
  }
  auto new_root = p;
  std::shared_ptr<TrieNode> parent;
  for (char ch : key) {
    parent = p;
    if (p->children_.count(ch)) {
      auto temp = std::shared_ptr<TrieNode>(p->children_[ch]->Clone());
      p->children_[ch] = temp;
      p = temp;
    } else {
      auto temp = std::make_shared<TrieNode>();
      p->children_.insert(std::make_pair(ch, temp));
      p = temp;
    }
  }
  auto leave = std::make_shared<TrieNodeWithValue<T>>(p->children_, std::make_shared<T>(std::move(value)));
  leave->is_value_node_ = true;
  if (key.empty()) {
    return Trie(leave);
  }
  parent->children_[key[key.size() - 1]] = leave;
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto p = std::shared_ptr<TrieNode>(root_->Clone());
  auto new_root = p;
  for (char ch : key) {
    auto temp = std::shared_ptr<TrieNode>(p->children_[ch]->Clone());
    p->children_[ch] = temp;
    p = temp;
  }
  p->is_value_node_ = false;
  if (RemoveHelper(key, 0, new_root)) {
    new_root = nullptr;
  }
  return Trie(new_root);
}

auto Trie::RemoveHelper(std::string_view key, size_t index, const std::shared_ptr<TrieNode> &p) const -> bool {
  if (index == key.length()) {
    return p->children_.empty();
  }
  bool flag = RemoveHelper(key, index + 1, const_pointer_cast<TrieNode>(p->children_[key[index]]));
  if (flag) {
    p->children_.erase(key[index]);
  }
  return (!p->is_value_node_ && p->children_.empty());
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
