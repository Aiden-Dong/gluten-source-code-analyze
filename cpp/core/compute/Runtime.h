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

#include <glog/logging.h>

#include "compute/ProtobufUtils.h"
#include "compute/ResultIterator.h"
#include "memory/ColumnarBatch.h"
#include "memory/MemoryManager.h"
#include "operators/c2r/ColumnarToRow.h"
#include "operators/r2c/RowToColumnar.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "substrait/plan.pb.h"
#include "utils/ObjectStore.h"
#include "utils/WholeStageDumper.h"

namespace gluten {

class ResultIterator;

struct SparkTaskInfo {
  int32_t stageId{0};
  int32_t partitionId{0};
  // Same as TID.
  int64_t taskId{0};
  // virtual id for each backend internal use
  int32_t vId{0};

  std::string toString() const {
    return "[Stage: " + std::to_string(stageId) + " TID: " + std::to_string(taskId) + " VID: " + std::to_string(vId) +"]";
  }

  friend std::ostream& operator<<(std::ostream& os, const SparkTaskInfo& taskInfo) {
    os << taskInfo.toString();
    return os;
  }
};


/********************************************************************
 * Runtime 类确实 Velox 后端的执行控制中心
 * VeloxRuntime 负责协调所有执行组件
 * 通过 createResultIterator() 创建实际的执行迭代器
 * 是 Java 层和 Velox 原生引擎之间的关键桥梁
 *
 * 1. parsePlan() → 解析查询计划
 * 2. parseSplitInfo() → 解析数据分片
 * 3. createResultIterator() → 创建执行迭代器
 * 4. 执行过程中调用各种 create* 方法创建所需组件
 * 5. getMetrics() → 收集执行指标
*********************************************************************/

class Runtime : public std::enable_shared_from_this<Runtime> {
 public:

  // 定义函数
  using Factory = std::function<Runtime*(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf)>;

  using Releaser = std::function<void(Runtime*)>;

  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);

  static Runtime* create(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf = {});

  static void release(Runtime*);
  static std::optional<std::string>* localWriteFilesTempPath();
  static std::optional<std::string>* localWriteFileName();

  Runtime(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& confMap)
      : kind_(kind), memoryManager_(memoryManager), confMap_(confMap) {}

  virtual ~Runtime() = default;

  virtual std::string kind() {
    return kind_;
  }

  /*****
   * 解析 Substrait 查询计划
   * • 接收从 Java 层传来的序列化 Substrait 计划
   * • 反序列化为 substrait::Plan 对象
   * • 支持调试模式下的 JSON 格式输出
   * • 是查询执行的第一步
   **/
  virtual void parsePlan(const uint8_t* data, int32_t size) {
    throw GlutenException("Not implemented");
  }

  /****
   * 解析数据分片信息
   * • 解析文件扫描的分片信息（文件路径、偏移量、长度等）
   * • 用于数据源读取时的分区处理
   * • 支持多个分片的并行处理
   **/
  virtual void parseSplitInfo(const uint8_t* data, int32_t size, int32_t idx) {
    throw GlutenException("Not implemented");
  }


  // 计划树结果打印
  virtual std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
    throw GlutenException("Not implemented");
  }

  // 获取计划树
  ::substrait::Plan& getPlan() {
    return substraitPlan_;
  }

  /***
   * 创建查询执行迭代器
   * • 将 Substrait 计划转换为 Velox 执行计划
   * • 创建 WholeStageResultIterator 执行整个查询阶段
   * • 处理输入迭代器（用于 Join、Union 等操作）
   * • 设置溢出目录用于大数据处理
   *
   **/
  virtual std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs) {
    throw GlutenException("Not implemented");
  }

  /****
   *  作用: 创建空模式的列式批次
   *  • 用于处理没有列的查询（如 SELECT COUNT(*)）
   *  • 缓存不同行数的空批次以提高性能
   *  • 避免重复创建相同的空批次
   */
  virtual std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) {
    throw GlutenException("Not implemented");
  }

  /******
   * 作用: 列选择操作
   * • 从批次中选择指定的列
   * • 用于投影操作的优化
   * • 避免不必要的列数据传输
   */
  virtual std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch>, const std::vector<int32_t>&) {
    throw GlutenException("Not implemented");
  }

  /***
   * 作用: 获取内存管理器
   * • 返回当前 Runtime 使用的内存管理器
   * • 用于分配和释放原生内存
   * • 支持内存池和内存监控
   **/
  virtual MemoryManager* memoryManager() {
    return memoryManager_;
  };

  /***
   * 作用: 创建列式到行式转换器
   * • 将 Velox 列式数据转换为 Spark 行式数据
   * • 用于回退到 Spark 原生操作时的数据转换
   * • 支持内存阈值控制
   **/
  virtual std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) {
    throw GlutenException("Not implemented");
  }

  /****
   * 作用: 创建行式到列式转换器
   * • 将 Spark 行式数据转换为 Velox 列式数据
   * • 用于接收 Spark 数据进行 Velox 处理
   * • 基于 Arrow Schema 进行转换
   **/
  virtual std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }

  /***
   * 作用: 创建 Shuffle 写入器
   * • 用于 Shuffle 阶段的数据写入
   * • 支持哈希分区和排序分区
   * • 处理数据的重新分布
   **/
  virtual std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options) {
    throw GlutenException("Not implemented");
  }

  virtual Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) {
    throw GlutenException("Not implemented");
  }

  /****
  * 作用: 创建 Shuffle 读取器
  * • 用于读取 Shuffle 阶段的数据
  * • 支持多个分区的数据合并
  * • 处理网络数据传输
  */
  virtual std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) {
    throw GlutenException("Not implemented");
  }

  virtual std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }

  const std::unordered_map<std::string, std::string>& getConfMap() {
    return confMap_;
  }

  /***
   * 作用: 设置 Spark 任务信息
   * • 存储当前任务的 Stage ID、Partition ID、Task ID
   * • 用于日志记录和调试
   * • 支持任务级别的资源隔离
   **/
  virtual void setSparkTaskInfo(SparkTaskInfo taskInfo) {
    taskInfo_ = taskInfo;
  }

  std::optional<SparkTaskInfo> getSparkTaskInfo() const {
    return taskInfo_;
  }

  virtual void enableDumping() {
    throw GlutenException("Not implemented");
  }

  virtual WholeStageDumper* getDumper() {
    return dumper_.get();
  }

  ObjectHandle saveObject(std::shared_ptr<void> obj) {
    return objStore_->save(obj);
  }

 protected:
  std::string kind_;
  MemoryManager* memoryManager_;
  std::unique_ptr<ObjectStore> objStore_ = ObjectStore::create();
  std::unordered_map<std::string, std::string> confMap_; // Session conf map

  ::substrait::Plan substraitPlan_;                         // 计划树-- 本质上就是一个ProtoBuf
  std::vector<::substrait::ReadRel_LocalFiles> localFiles_;

  std::optional<SparkTaskInfo> taskInfo_{std::nullopt};
  std::shared_ptr<WholeStageDumper> dumper_{nullptr};
};
} // namespace gluten