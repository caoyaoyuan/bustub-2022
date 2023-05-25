//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <exception>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0U) {
    return false;
  }
  bool flag = false;
  if (!history_list_.empty()) {
    for (auto iter = history_list_.rbegin(); iter != history_list_.rend(); ++iter) {
      if (evictable_[*iter]) {
        *frame_id = *iter;
        evictable_.erase(*iter);
        history_cnt_.erase(*iter);
        --curr_size_;
        RemoveNodeHistory(*iter);
        flag = true;
        break;
      }
    }
  }
  if (!flag && !buffer_list_.empty()) {
    for (auto iter = buffer_list_.rbegin(); iter != buffer_list_.rend(); ++iter) {
      if (evictable_[*iter]) {
        *frame_id = *iter;
        evictable_.erase(*iter);
        history_cnt_.erase(*iter);
        --curr_size_;
        flag = true;
        RemoveNodeBuffer(*iter);
        break;
      }
    }
  }
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");
  if (history_cnt_.find(frame_id) == history_cnt_.end()) {
    bool flag = false;
    if (IsFull()) {
      if (!history_list_.empty()) {
        for (auto iter = history_list_.rbegin(); iter != history_list_.rend(); ++iter) {
          if (evictable_[*iter]) {
            evictable_.erase(*iter);
            history_cnt_.erase(*iter);
            --curr_size_;
            RemoveNodeHistory(*iter);
            flag = true;
            break;
          }
        }
      }
      if (!flag && !buffer_list_.empty()) {
        for (auto iter = buffer_list_.rbegin(); iter != buffer_list_.rend(); ++iter) {
          if (evictable_[*iter]) {
            evictable_.erase(*iter);
            history_cnt_.erase(*iter);
            --curr_size_;
            RemoveNodeBuffer(*iter);
            flag = true;
            break;
          }
        }
      }
      if (!flag) {
        return;
      }
    }
    InsertHistory(frame_id);
    evictable_[frame_id] = false;
  }

  ++history_cnt_[frame_id];
  if (history_cnt_[frame_id] >= k_) {
    RemoveNodeHistory(frame_id);
    InsertBuffer(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");
  // if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
  //   throw std::exception();
  // }
  if (history_cnt_[frame_id] == 0) {
    return;
  }
  if (evictable_[frame_id] && !set_evictable) {
    curr_size_--;
    evictable_[frame_id] = false;
  } else if (!evictable_[frame_id] && set_evictable) {
    curr_size_++;
    evictable_[frame_id] = true;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");
  if (history_cnt_.find(frame_id) == history_cnt_.end()) {
    return;
  }
  // 出错点
  auto cnt = history_cnt_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    RemoveNodeHistory(frame_id);
  } else {
    RemoveNodeBuffer(frame_id);
  }
  curr_size_--;
  history_cnt_.erase(frame_id);
  evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

auto LRUKReplacer::BufferSize() -> size_t { return buffer_list_.size(); }

auto LRUKReplacer::HistorySize() -> size_t { return history_list_.size(); }
}  // namespace bustub
