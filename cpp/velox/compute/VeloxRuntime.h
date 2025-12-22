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

#include "WholeStageResultIterator.h"
#include "compute/Runtime.h"
#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "iceberg/IcebergWriter.h"
#endif
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "operators/writer/VeloxParquetDataSource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "IcebergNestedField.pb.h"
#endif

namespace gluten {

class VeloxRuntime final : public Runtime {
 public:
  explicit VeloxRuntime(
      const std::string& kind,
      VeloxMemoryManager* vmm,
      const std::unordered_map<std::string, std::string>& confMap);

  void setSparkTaskInfo(SparkTaskInfo taskInfo) override {
    static std::atomic<uint32_t> vtId{0};
    taskInfo.vId = vtId++;
    taskInfo_ = taskInfo;
  }

  /***
   * 解析执行计划
   * • **协议解析**：将 Protobuf 格式的 Substrait 计划解析为内部结构
   * • **调试输出**：在调试模式下输出计划的 JSON 表示
   * • **计划存储**：解析后存储在 substraitPlan_ 中（继承自父类）
   **/
  void parsePlan(const uint8_t* data, int32_t size) override;

  void parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) override;

  /***
   * 获取内存管理器
   * • **内存访问**：返回 VeloxMemoryManager 实例
   * • **类型转换**：将父类的 MemoryManager 转换为 VeloxMemoryManager
   * • **内存统一**：提供统一的内存管理接口
   */
  VeloxMemoryManager* memoryManager() override;

  // FIXME This is not thread-safe?

  /***
   * 创建结果迭代器
   * • **核心执行**：创建 WholeStageResultIterator 执行 Velox 计划
   * • **计划转换**：将 Substrait 计划转换为 Velox 计划
   * • **任务创建**：基于计划和分片信息创建 Velox Task
   * • **结果封装**：返回可迭代的结果接口
   **/
  std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs = {}) override;

  /****
   * 创建列转行转换器
   * • **格式转换**：创建 VeloxColumnarToRowConverter
   * • **内存控制**：设置转换过程的内存阈值
   * • **Fallback 支持**：支持回退到 Spark 行式处理
   */
  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) override;

  /****
   * 创建空模式批次
   * • **缓存查找**：先从 emptySchemaBatchLoopUp_ 查找
   * • **按需创建**：不存在时创建新的空模式批次
   * • **性能优化**：避免重复创建相同的空批次
   */
  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override;

  std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch> batch, const std::vector<int32_t>& columnIndices)
      override;

  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) override;

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
  std::shared_ptr<IcebergWriter> createIcebergWriter(
      RowTypePtr rowType,
      int32_t format,
      const std::string& outputDirectory,
      facebook::velox::common::CompressionKind compressionKind,
      std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const gluten::IcebergNestedField& protoField,
      const std::unordered_map<std::string, std::string>& sparkConfs);
#endif

  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options) override;

  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) override;

  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) override;

  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override;

  void enableDumping() override;

  std::shared_ptr<VeloxDataSource> createDataSource(const std::string& filePath, std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<const facebook::velox::core::PlanNode> getVeloxPlan() {
    return veloxPlan_;
  }

  bool debugModeEnabled() const {
    return debugModeEnabled_;
  }

  static void getInfoAndIds(
      const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
      const std::unordered_set<facebook::velox::core::PlanNodeId>& leafPlanNodeIds,
      std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      std::vector<facebook::velox::core::PlanNodeId>& scanIds,
      std::vector<facebook::velox::core::PlanNodeId>& streamIds);

 private:
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;     // Velox 执行计划
  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_;        // 存储 Velox 引擎的所有配置参数
  bool debugModeEnabled_{false};                                         // 调试模式标志
  std::unordered_map<int32_t, std::shared_ptr<VeloxColumnarBatch>> emptySchemaBatchLoopUp_;  // 空模式批次缓存
};

} // namespace gluten
