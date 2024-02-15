#include "primer/trie_store.h"
#include <memory>
#include <mutex>
#include "common/exception.h"

namespace bustub {
#pragma clang diagnostic ignored "-Wc++20-extensions"
#pragma clang diagnostic ignored "-Wc++11-extensions"
template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.a
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // if (!root_.GetRoot()) {
  //   return std::nullopt;
  // }

  // std::shared_ptr<const TrieNode> p;
  std::unique_lock<std::mutex> locker(root_lock_);
  // p = root_.GetRoot();
  Trie trie = root_;
  locker.unlock();
  const T *ret = trie.Get<T>(key);
  if (ret == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(trie, *ret);
  // auto old_root = p;
  // for (char ch : key) {
  //   if (!p->children_.count(ch)) {
  //     return std::nullopt;
  //   }
  //   //std::shared_ptr<TrieNode> temp = p->children_[ch]->Clone();
  //   //p->children_[ch] = temp;
  //   //p = temp;
  //   p = p->children_.at(ch);
  // }
  // auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(p.get());
  // if (!ret || !p->is_value_node_) {
  //   return std::nullopt;
  // }
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // std::lock_guard<std::mutex> lock(write_lock_);
  // std::shared_ptr<TrieNode> p;
  // {
  //   std::lock_guard<std::mutex> locker(root_lock_);
  //   if (!root_.GetRoot()) {
  //     p = std::make_shared<TrieNode>();
  //   } else {
  //     p = std::shared_ptr<TrieNode>(root_.GetRoot()->Clone());
  //   }
  // }

  // auto new_root = p;
  // auto parent = p;
  // for (char ch : key) {
  //   parent = p;
  //   if (p->children_.count(ch)) {
  //     std::shared_ptr<TrieNode> temp = p->children_[ch]->Clone();
  //     p->children_[ch] = temp;
  //     p = temp;
  //   } else {
  //     auto temp = std::make_shared<TrieNode>();
  //     p->children_.insert(std::make_pair(ch, temp));
  //     p = temp;
  //   }
  // }
  // auto leave = std::make_shared<TrieNodeWithValue<T>>(p->children_, std::make_shared<T>(std::move(value)));
  // leave->is_value_node_ = true;
  // parent->children_[key[key.size() - 1]] = leave;
  // std::lock_guard<std::mutex> locker(root_lock_);
  // root_ = Trie(new_root);

  std::lock_guard<std::mutex> lock(write_lock_);
  std::unique_lock<std::mutex> locker(root_lock_);
  Trie root = root_;
  locker.unlock();
  Trie ret = root.Put<T>(key, std::move(value));
  locker.lock();
  root_ = ret;
  locker.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // std::lock_guard<std::mutex> lock(write_lock_);
  // std::shared_ptr<TrieNode> p;
  // {
  //   std::lock_guard<std::mutex> locker(root_lock_);
  //   p = std::shared_ptr<TrieNode>(root_.GetRoot()->Clone());
  // }
  // auto new_root = p;
  // for (char ch : key) {
  //   std::shared_ptr<TrieNode> tmp = p->children_[ch]->Clone();
  //   p->children_[ch] = tmp;
  //   p = tmp;
  // }
  // p->is_value_node_ = false;
  // if (RemoveHelper(key, 0, new_root)) {
  //   new_root = nullptr;
  // }
  // std::lock_guard<std::mutex> locker(root_lock_);
  // root_ = Trie(new_root);
  std::lock_guard<std::mutex> lock(write_lock_);
  std::unique_lock<std::mutex> locker(root_lock_);
  Trie root = root_;
  locker.unlock();

  Trie ret = root.Remove(key);

  locker.lock();
  root_ = ret;
  locker.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
