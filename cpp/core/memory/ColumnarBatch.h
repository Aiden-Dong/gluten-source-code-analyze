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

#include <memory>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/record_batch.h"
#include "memory/MemoryManager.h"
#include "utils/ArrowStatus.h"
#include "utils/Exception.h"

namespace gluten {

/***
 * • **数据载体**：在 JVM 和 Native 引擎之间传递列式数据
 * • **格式桥接**：连接 Spark 的 ColumnarBatch 和 Arrow 数据格式
 * • **内存管理**：与 Gluten 内存管理系统集成
 * • **性能优化**：避免行列转换，保持列式处理优势
 */
class ColumnarBatch {
 public:
  ColumnarBatch(int32_t numColumns, int32_t numRows);

  virtual ~ColumnarBatch() = default;

  // 返回批次中的列数
  int32_t numColumns() const;

  // 返回批次中的行数
  int32_t numRows() const;

  // 返回批次类型标识
  virtual std::string getType() const = 0;

  // 计算批次占用的内存字节数
  virtual int64_t numBytes() = 0;

  // 导出为 Arrow C 数据接口的 ArrowArray 结构
  virtual std::shared_ptr<ArrowArray> exportArrowArray() = 0;

  // 导出为 Arrow C 数据接口的 ArrowSchema 结构
  virtual std::shared_ptr<ArrowSchema> exportArrowSchema() = 0;

  virtual int64_t getExportNanos() const;

  // 将指定行转换为 Spark UnsafeRow 格式的字节数组
  virtual std::vector<char> toUnsafeRow(int32_t rowId) const;

  friend std::ostream& operator<<(std::ostream& os, const ColumnarBatch& columnarBatch);

 private:
  int32_t numColumns_;
  int32_t numRows_;

 protected:
  int64_t exportNanos_;
};

class ArrowColumnarBatch final : public ColumnarBatch {
 public:
  // 接收 Arrow RecordBatch 并构造 ArrowColumnarBatch
  explicit ArrowColumnarBatch(std::shared_ptr<arrow::RecordBatch> batch);   // explicit 防止别隐式转换构造

  std::string getType() const override;     //  batch 类型标识

  int64_t numBytes() override;

  arrow::RecordBatch* getRecordBatch() const;                    // 返回底层 Arrow RecordBatch 的原始指针
  std::shared_ptr<ArrowSchema> exportArrowSchema() override;     // 将 Arrow Schema 导出为 C 数据接口格式
  std::shared_ptr<ArrowArray> exportArrowArray() override;       //  将 RecordBatch 数据导出为 C 数据接口格式

  std::vector<char> toUnsafeRow(int32_t rowId) const override;

 private:
  std::shared_ptr<arrow::RecordBatch> batch_;                       // 存储底层的 Arrow RecordBatch 数据
};

class ArrowCStructColumnarBatch final : public ColumnarBatch {
 public:
  ArrowCStructColumnarBatch(std::unique_ptr<ArrowSchema> cSchema, std::unique_ptr<ArrowArray> cArray);

  ~ArrowCStructColumnarBatch() override;

  std::string getType() const override;

  int64_t numBytes() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;

  std::shared_ptr<ArrowArray> exportArrowArray() override;

  std::vector<char> toUnsafeRow(int32_t rowId) const override;

 private:
  std::shared_ptr<ArrowSchema> cSchema_ = std::make_shared<ArrowSchema>();
  std::shared_ptr<ArrowArray> cArray_ = std::make_shared<ArrowArray>();
};

std::shared_ptr<ColumnarBatch> createZeroColumnBatch(int32_t numRows);

} // namespace gluten
