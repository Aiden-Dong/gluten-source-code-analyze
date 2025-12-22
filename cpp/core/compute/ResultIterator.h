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

#include "memory/ColumnarBatch.h"
#include "memory/ColumnarBatchIterator.h"
#include "utils/Metrics.h"

namespace gluten {

class Runtime;

// FIXME the code is tightly coupled with Velox plan execution. Should cleanup the abstraction for uses from
//  other places.

/****
 * ColumnarBatchIterator (抽象基类)
 *      ↓
 *  WholeStageResultIterator (Velox具体实现)
 *      ↓
 *  ResultIterator (包装器类)
 **/
class ResultIterator {
 public:
  explicit ResultIterator(std::unique_ptr<ColumnarBatchIterator> iter, Runtime* runtime = nullptr)
      : iter_(std::move(iter)), next_(nullptr), runtime_(runtime) {}

  // copy constructor and copy assignment (deleted)
  ResultIterator(const ResultIterator& in) = delete;
  ResultIterator& operator=(const ResultIterator&) = delete;

  // move constructor and move assignment
  ResultIterator(ResultIterator&& in) = default;
  ResultIterator& operator=(ResultIterator&& in) = default;

  // 检查是否有下一个批次
  bool hasNext() {
    checkValid();
    getNext();
    return next_ != nullptr;
  }

  // 获取下一个批次
  std::shared_ptr<ColumnarBatch> next() {
    checkValid();
    getNext();
    return std::move(next_);
  }

  // For testing and benchmarking.
  ColumnarBatchIterator* getInputIter() {
    return iter_.get();
  }

  // 获取执行指标
  Metrics* getMetrics();

  // 设置导出时间
  void setExportNanos(int64_t exportNanos) {
    exportNanos_ = exportNanos;
  }


  int64_t getExportNanos() const {
    return exportNanos_;
  }

  // 溢出处理
  int64_t spillFixedSize(int64_t size) {
    return iter_->spillFixedSize(size);
  }

 private:
  void checkValid() const {
    if (iter_ == nullptr) {
      throw GlutenException("ResultIterator: the underlying iterator has expired.");
    }
  }

  // 懒加载下一个批次
  void getNext() {
    if (next_ == nullptr) {
      next_ = iter_->next();
    }
  }

  std::unique_ptr<ColumnarBatchIterator> iter_;    // 底层迭代器
  std::shared_ptr<ColumnarBatch> next_;            // 缓存的下一个批次
  Runtime* runtime_;                               // 运行时上下文
  int64_t exportNanos_;
};

} // namespace gluten
