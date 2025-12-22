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

#include "arrow/memory_pool.h"

#include "MemoryAllocator.h"

namespace gluten {

using ArrowMemoryPoolReleaser = std::function<void(arrow::MemoryPool*)>;

// • 实现 arrow::MemoryPool 接口，为 Arrow 库提供统一的内存管理
// • 将 Arrow 的内存分配请求桥接到 Gluten 的内存管理系统。
// • **桥接层**：连接 Arrow C++ 库和 Gluten 内存管理系统
// • **统一管理**：确保 Arrow 数据结构的内存分配也受 Gluten 内存策略控制
// • **监控集成**：Arrow 的内存使用纳入整体内存监控体系
class ArrowMemoryPool final : public arrow::MemoryPool {
 public:
  explicit ArrowMemoryPool(AllocationListener* listener, ArrowMemoryPoolReleaser releaser = nullptr)
      : allocator_(std::make_unique<ListenableMemoryAllocator>(defaultMemoryAllocator().get(), listener)),
        releaser_(std::move(releaser)) {}

  ~ArrowMemoryPool() override;

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

  std::string backend_name() const override;

  MemoryAllocator* allocator() const;

 private:
  std::unique_ptr<MemoryAllocator> allocator_;
  ArrowMemoryPoolReleaser releaser_;
};

} // namespace gluten
