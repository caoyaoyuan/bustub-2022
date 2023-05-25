//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  std::shared_ptr<Bucket> tar_bucket = dir_[index];
  return tar_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  std::shared_ptr<Bucket> tar_bucket = dir_[index];
  return tar_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  std::shared_ptr<Bucket> tar_bucket = dir_[index];
  auto mask = 1 << tar_bucket->GetDepth();
  V val;
  if (tar_bucket->Find(key, val)) {
    if (val == value) {
      return;
    }
    tar_bucket->Remove(key);
  }

  while (dir_[IndexOf(key)]->IsFull()) {
    index = IndexOf(key);
    tar_bucket = dir_[index];
    mask = 1 << tar_bucket->GetDepth();
    if (GetLocalDepthInternal(index) == GetGlobalDepthInternal()) {
      global_depth_++;
      int len = dir_.size();
      for (int i = 0; i < len; ++i) {
        dir_.push_back(dir_[i]);
      }
    }
    std::shared_ptr<Bucket> bucket0 = std::make_shared<Bucket>(bucket_size_, tar_bucket->GetDepth() + 1);
    std::shared_ptr<Bucket> bucket1 = std::make_shared<Bucket>(bucket_size_, tar_bucket->GetDepth() + 1);
    num_buckets_++;
    for (const auto &[oldKey, oldValue] : tar_bucket->GetItems()) {
      auto hash_key = std::hash<K>()(oldKey);
      if ((hash_key & mask) != 0) {
        bucket1->Insert(oldKey, oldValue);
      } else {
        bucket0->Insert(oldKey, oldValue);
      }
    }
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == tar_bucket) {
        if ((i & mask) != 0) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket0;
        }
      }
    }
  }

  index = IndexOf(key);
  tar_bucket = dir_[index];
  tar_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &iter : list_) {
    if (iter.first == key) {
      value = iter.second;
      return true;
    }
  }
  value = {};
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // return std::remove_if(list_.begin(), list_.end(), [&key](const auto &iter) { return iter.first == key; }) !=
  //        list_.end();
  V val;
  if (Find(key, val)) {
    list_.remove({key, val});
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &iter : list_) {
    if (iter.first == key) {
      iter.second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
