/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <memory>
#include <mutex>

namespace gluten {

// 是 Gluten 内存管理系统中的**内存分配监听器**，用于跟踪和监控内存分配变化
class AllocationListener {
 public:
  static std::unique_ptr<AllocationListener> noop();

  virtual ~AllocationListener() = default;

  // 内存分配变化通知 - 核心方法
  virtual void allocationChanged(int64_t diff) = 0;

  // 获取当前已分配字节数
  virtual int64_t currentBytes() {
    return 0;
  }

  // 获取峰值内存使用量
  virtual int64_t peakBytes() {
    return 0;
  }

 protected:
  AllocationListener() = default;
};

/// Memory changes will be round to specified block size which aim to decrease delegated listener calls.
// The class must be thread safe
class BlockAllocationListener final : public AllocationListener {
 public:
  BlockAllocationListener(AllocationListener* delegated, int64_t blockSize)
      : delegated_(delegated), blockSize_(blockSize) {}

  void allocationChanged(int64_t diff) override {
    if (diff == 0) {
      return;
    }
    int64_t granted = reserve(diff);
    if (granted == 0) {
      return;
    }
    try {
      delegated_->allocationChanged(granted);
    } catch (const std::exception&) {
      reserve(-diff);
      throw;
    }
  }

  int64_t currentBytes() override {
    return reservationBytes_;
  }

  int64_t peakBytes() override {
    return peakBytes_;
  }

 private:
  // 核心预留算法
  inline int64_t reserve(int64_t diff) {

    std::lock_guard<std::mutex> lock(mutex_);      // 加锁保护

    usedBytes_ += diff;                               // 更新实际使用量
    int64_t newBlockCount;
    if (usedBytes_ == 0) {
      newBlockCount = 0;
    } else {
      // ceil to get the required block number
      newBlockCount = (usedBytes_ - 1) / blockSize_ + 1;  // 向上取整计算需要的块数
    }
    int64_t bytesGranted = (newBlockCount - blocksReserved_) * blockSize_;

    blocksReserved_ = newBlockCount;                     // 更新预留块数
    peakBytes_ = std::max(peakBytes_, usedBytes_); // 更新峰值
    return bytesGranted;                                 // 返回需要申请/释放的字节数
  }

  AllocationListener* const delegated_;    // 指向更高层的内存监听器（如连接到 VeloxMemoryManager）
  const uint64_t blockSize_;               // 内存变化聚合的基本单位（如 64KB、1MB）
  int64_t blocksReserved_{0L};             // 记录当前预留了多少个内存块
  int64_t usedBytes_{0L};                  // 记录实际使用的字节数（不是块对齐的）
  int64_t peakBytes_{0L};                  // 记录使用过的最大内存量
  int64_t reservationBytes_{0L};           // 当前向上级监听器预留的总字节数

  mutable std::mutex mutex_;
};

} // namespace gluten
