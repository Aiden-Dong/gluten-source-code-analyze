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

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>

#include "memory/AllocationListener.h"

namespace gluten {

/***
 * 定义统一的内存分配接口
 * 为 Gluten 提供可插拔的内存管理策略
 */
class MemoryAllocator {
 public:
  virtual ~MemoryAllocator() = default;

  /***
   * 分配指定大小的内存块
   * - size：要分配的字节数
   * - out：输出参数，成功时存储分配的内存地址指针
   */
  virtual bool allocate(int64_t size, void** out) = 0;

  /***
   * 分配内存并初始化为零
   * • nmemb：元素个数
   * • size：每个元素的字节数
   * • out：输出参数，存储分配的内存地址指针
   */
  virtual bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) = 0;

  /****
  * 功能：分配按指定边界对齐的内存
  * 参数：
  * • alignment：内存对齐边界（必须是2的幂，如8、16、32、64）
  * • size：要分配的字节数
  * • out：输出参数，存储分配的内存地址指针
  * */
  virtual bool allocateAligned(uint64_t alignment, int64_t size, void** out) = 0;

  /****
  * 调整已分配内存块的大小
  * 参数：
  * • p：原内存块指针
  * • size：原内存块大小
  * • newSize：新的内存块大小
  * • out：输出参数，存储重新分配后的内存地址指针
  * */
  virtual bool reallocate(void* p, int64_t size, int64_t newSize, void** out) = 0;
  virtual bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) = 0;

  // 释放内存
  virtual bool free(void* p, int64_t size) = 0;

  virtual int64_t getBytes() const = 0;

  virtual int64_t peakBytes() const = 0;
};

// 包装其他分配器，添加监听功能
class ListenableMemoryAllocator final : public MemoryAllocator {
 public:
  explicit ListenableMemoryAllocator(MemoryAllocator* delegated, AllocationListener* listener)
      : delegated_(delegated), listener_(listener) {}

 public:
  bool allocate(int64_t size, void** out) override;

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override;

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override;

  bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) override;

  bool free(void* p, int64_t size) override;

  int64_t getBytes() const override;

  int64_t peakBytes() const override;

 private:
  void updateUsage(int64_t size);
  MemoryAllocator* const delegated_;      // 指向真正执行内存分配操作的底层分配器
  AllocationListener* const listener_;    // 接收内存分配变化的通知
  std::atomic_int64_t usedBytes_{0L};  // 当前使用量
  std::atomic_int64_t peakBytes_{0L};  // 峰值使用量
};

/***
 * • 基于标准 C 库的内存分配实现
 * • 提供基础的 malloc/free 功能
 * • 包含使用量统计
 */
class StdMemoryAllocator final : public MemoryAllocator {
 public:
  bool allocate(int64_t size, void** out) override;

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override;

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override;

  bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) override;

  bool free(void* p, int64_t size) override;

  int64_t getBytes() const override;

  int64_t peakBytes() const override;

 private:
  std::atomic_int64_t bytes_{0};
};

std::shared_ptr<MemoryAllocator> defaultMemoryAllocator();

} // namespace gluten
