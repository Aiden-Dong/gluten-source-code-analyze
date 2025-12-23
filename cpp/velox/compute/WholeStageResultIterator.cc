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
#include "WholeStageResultIterator.h"
#include "VeloxBackend.h"
#include "VeloxRuntime.h"
#include "config/VeloxConfig.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/PlanNodeStats.h"
#ifdef GLUTEN_ENABLE_GPU
#include <cudf/io/types.hpp>
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnectorSplit.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "cudf/GpuLock.h"
#endif

using namespace facebook;

namespace gluten {

namespace {

// metrics
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows = "replacedWithDynamicFilterRows";
const std::string kFlushRowCount = "flushRowCount";
const std::string kLoadedToValueHook = "loadedToValueHook";
const std::string kTotalScanTime = "totalScanTime";
const std::string kSkippedSplits = "skippedSplits";
const std::string kProcessedSplits = "processedSplits";
const std::string kSkippedStrides = "skippedStrides";
const std::string kProcessedStrides = "processedStrides";
const std::string kRemainingFilterTime = "totalRemainingFilterTime";
const std::string kIoWaitTime = "ioWaitWallNanos";
const std::string kStorageReadBytes = "storageReadBytes";
const std::string kLocalReadBytes = "localReadBytes";
const std::string kRamReadBytes = "ramReadBytes";
const std::string kPreloadSplits = "readyPreloadedSplits";
const std::string kPageLoadTime = "pageLoadTimeNs";
const std::string kDataSourceAddSplitWallNanos = "dataSourceAddSplitWallNanos";
const std::string kWaitForPreloadSplitNanos = "waitForPreloadSplitNanos";
const std::string kDataSourceReadWallNanos = "dataSourceReadWallNanos";
const std::string kNumWrittenFiles = "numWrittenFiles";
const std::string kWriteIOTime = "writeIOWallNanos";

// others
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

} // namespace

WholeStageResultIterator::WholeStageResultIterator(
    VeloxMemoryManager* memoryManager,                                                // 内存管理器指针
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,           // Velox执行计划
    const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,                // 扫描节点ID列表
    const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,                         // 数据分片信息
    const std::vector<facebook::velox::core::PlanNodeId>& streamIds,                  // 流节点ID列表
    const std::string spillDir,                                                       // 溢出目录路径
    const facebook::velox::config::ConfigBase* veloxCfg,                              // Velox配置
    const SparkTaskInfo& taskInfo)                                                    // Spark任务信息
    : memoryManager_(memoryManager),
      veloxCfg_(veloxCfg),
#ifdef GLUTEN_ENABLE_GPU
      enableCudf_(veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)),           // GPU支持标志
#endif
      taskInfo_(taskInfo),
      veloxPlan_(planNode),
      scanNodeIds_(scanNodeIds),
      scanInfos_(scanInfos),
      streamIds_(streamIds) {
  // 从配置中获取溢出策略，默认值为 kSpillStrategyDefaultValue
  spillStrategy_ = veloxCfg_->get<std::string>(kSpillStrategy, kSpillStrategyDefaultValue);

  // 获取溢出线程数配置
  auto spillThreadNum = veloxCfg_->get<uint32_t>(kSpillThreadNum, kSpillThreadNumDefaultValue);
  // 如果配置了溢出线程数 > 0，创建CPU线程池执行器用于异步溢出
  if (spillThreadNum > 0) {
    spillExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(spillThreadNum);
  }

  // 遍历执行计划，按拓扑顺序获取所有节点ID，用于后续指标收集
  getOrderedNodeIds(veloxPlan_, orderedNodeIds_);

  //  根据溢出目录路径获取对应的文件系统（可能是本地FS、HDFS、S3等
  auto fileSystem = velox::filesystems::getFileSystem(spillDir, nullptr);
  GLUTEN_CHECK(fileSystem != nullptr, "File System for spilling is null!");
  fileSystem->mkdir(spillDir);  // 创建溢出目录（如果不存在)

  // 配置溢出磁盘选项：路径、已创建标志、创建回调
  velox::common::SpillDiskOptions spillOpts{
      .spillDirPath = spillDir, .spillDirCreated = true, .spillDirCreateCb = nullptr};

  //////    Velox Task创建              /////

  // 创建空的节点ID集合
  std::unordered_set<velox::core::PlanNodeId> emptySet;
  // 创建计划片段：执行计划、非分组策略、1个分区、空依赖集合
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1, emptySet};
  // 创建新的Velox查询上下文（包含内存池、配置等）
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx();

  // 构建 velox 任务执行流程
  task_ = velox::exec::Task::create(
      fmt::format("Gluten_Stage_{}_TID_{}_VTID_{}",std::to_string(taskInfo_.stageId),std::to_string(taskInfo_.taskId),std::to_string(taskInfo.vId)),             // 任务名称格式化
      std::move(planFragment),                   // 移动计划片段
      0,                                             // 目标分区ID
      std::move(queryCtx),                       // 移动查询上下文
      velox::exec::Task::ExecutionMode::kSerial,     // 串行执行模式
      /*consumer=*/velox::exec::Consumer{},          // 空消费
      /*memoryArbitrationPriority=*/0,               // 内存仲裁优先级
      /*spillDiskOpts=*/spillOpts,                // 溢出选项
      /*onError=*/nullptr);                       // 错误回调


  // 验证任务是否支持串行执行模式，不支持则抛出异常
  if (!task_->supportSerialExecutionMode()) {
    throw std::runtime_error("Task doesn't support single threaded execution: " + planNode->toString());
  }

  // 为分片向量预分配空间，避免动态扩容.
  splits_.reserve(scanInfos.size());
  if (scanNodeIds.size() != scanInfos.size()) {
    throw std::runtime_error("Invalid scan information.");
  }

  // 遍历所有的分片信息
  for (const auto& scanInfo : scanInfos) {
    // Get the information for TableScan.
    // Partition index in scan info is not used.
    const auto& paths = scanInfo->paths;                                            // 文件路径列表
    const auto& starts = scanInfo->starts;                                       // 文件读取起始位置
    const auto& lengths = scanInfo->lengths;                                     // 文件读取长度
    const auto& properties = scanInfo->properties;                     // 文件属性（大小、修改时间等
    const auto& format = scanInfo->format;                                                            // 文件格式 (ORC/Parquet)等
    const auto& partitionColumns = scanInfo->partitionColumns;    // 分区列信息
    const auto& metadataColumns = scanInfo->metadataColumns;      // 元数据列信息

#ifdef GLUTEN_ENABLE_GPU
    // Under the pre-condition that all the split infos has same partition column and format.
    const auto canUseCudfConnector = scanInfo->canUseCudfConnector();                                 // 是否可使用CUDF连接器
#endif

    // 为每个扫描信息创建连接器分片向量，预分配空间
    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> connectorSplits;
    connectorSplits.reserve(paths.size());      // 预分配空间

    // // 📝 为每个文件构建分区键映射
    for (int idx = 0; idx < paths.size(); idx++) {

      // 当前 schema 信息
      auto metadataColumn = metadataColumns[idx];
      std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
      if (!partitionColumns.empty()) {
        auto partitionColumn = partitionColumns[idx];
        constructPartitionColumns(partitionKeys, partitionColumn);
      }

      std::shared_ptr<velox::connector::ConnectorSplit> split;
      // iceberg split 处理
      if (auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(scanInfo)) {
        // Set Iceberg split.
        std::unordered_map<std::string, std::string> customSplitInfo{{"table_format", "hive-iceberg"}};
        auto deleteFiles = icebergSplitInfo->deleteFilesVec[idx];
        split = std::make_shared<velox::connector::hive::iceberg::HiveIcebergSplit>(
            kHiveConnectorId,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::nullopt,
            customSplitInfo,
            nullptr,
            true,
            deleteFiles,
            std::unordered_map<std::string, std::string>(),
            properties[idx]);
      } else {

        // Hive Split 处理

        auto connectorId = kHiveConnectorId;
#ifdef GLUTEN_ENABLE_GPU
        if (canUseCudfConnector && enableCudf_ &&
            veloxCfg_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault)) {
          connectorId = kCudfHiveConnectorId;
          VELOX_CHECK_EQ(starts[idx], 0, "Not support split file");
          VELOX_CHECK_EQ(lengths[idx], scanInfo->properties[idx]->fileSize, "Not support split file");
        }
#endif
        split = std::make_shared<velox::connector::hive::HiveConnectorSplit>(
            connectorId,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::nullopt /*tableBucketName*/,
            std::unordered_map<std::string, std::string>(),
            nullptr,
            std::unordered_map<std::string, std::string>(),
            0,
            true,
            metadataColumn,
            properties[idx]);
      }
      connectorSplits.emplace_back(split);
    }

    std::vector<velox::exec::Split> scanSplits;
    scanSplits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      // Bucketed group id (-1 means 'none').
      int32_t groupId = -1;
      scanSplits.emplace_back(velox::exec::Split(folly::copy(connectorSplit), groupId));
    }
    splits_.emplace_back(scanSplits);
  }
}

// 创建  Velox 查询上下文
std::shared_ptr<velox::core::QueryCtx> WholeStageResultIterator::createNewVeloxQueryCtx() {

  // connector 配置信息
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = createConnectorConfig();


  // Query Context 能力创建
  std::shared_ptr<velox::core::QueryCtx> ctx = velox::core::QueryCtx::create(
      nullptr,
      facebook::velox::core::QueryConfig{getQueryContextConf()},         // Query Context 配置信息
      connectorConfigs,                                                      // Connector 配置信息
      gluten::VeloxBackend::get()->getAsyncDataCache(),
      memoryManager_->getAggregateMemoryPool(),                                 // 获取聚合内存资源池
      spillExecutor_.get(),
      fmt::format(
          "Gluten_Stage_{}_TID_{}_VTID_{}",
          std::to_string(taskInfo_.stageId),
          std::to_string(taskInfo_.taskId),
          std::to_string(taskInfo_.vId)));
  return ctx;
}

// 执行整个Stage的计算
std::shared_ptr<ColumnarBatch> WholeStageResultIterator::next() {
  // // 步骤1: 尝试将分片添加到Velox任务
  // // 这是懒加载机制，只在需要时才添加分片，避免一次性加载所有数据
  tryAddSplitsToTask();         // 添加 Scan Split 计划

  //  步骤2: 检查任务是否已完成
  // 如果所有数据都已处理完毕，返回nullptr表示迭代结束
  if (task_->isFinished()) {
    return nullptr;
  }
  // 步骤3: 从Velox任务获取下一个数据向量s
  velox::RowVectorPtr vector;
  while (true) {
    // 创建一个空的Future对象，用于异步操作控制
    auto future = velox::ContinueFuture::makeEmpty();
    // // 调用Velox任务的next方法获取数据
    // future参数用于异步控制：如果数据未准备好，future会被设置
    auto out = task_->next(&future);

    // // 检查是否需要等待
    if (!future.valid()) {
      // Not need to wait. Break.
      // // future无效表示数据已准备好，无需等待
      vector = std::move(out);
      break;
    }
    // 如果到这里，说明Velox建议等待（通常是因为后台线程在溢出数据）
    GLUTEN_CHECK(out == nullptr, "Expected to wait but still got non-null output from Velox task");
    VLOG(2) << "Velox task " << task_->taskId()
            << " is busy when ::next() is called. Will wait and try again. Task state: "
            << taskStateString(task_->state());
    // 阻塞等待直到操作完成（如溢出完成）
    future.wait();
    // 等待完成后继续循环，重新尝试获取数据
  }

  // // 步骤4: 处理空结果情况
  if (vector == nullptr) {
    return nullptr;   // 没有更多数据
  }

  uint64_t numRows = vector->size();
  if (numRows == 0) {
    return nullptr;
  }

  {
    ScopedTimer timer(&loadLazyVectorTime_);
    for (auto& child : vector->children()) {
      child->loadedVector();
    }
  }

  // 步骤6: 包装并返回结果
  return std::make_shared<VeloxColumnarBatch>(vector);
}

int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  auto pool = memoryManager_->getAggregateMemoryPool();
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Spill[" + poolName + "]: "};
  int64_t shrunken = memoryManager_->shrink(size);
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    LOG(INFO) << fmt::format("{} trying to request spill for {}.", logPrefix, velox::succinctBytes(remaining));
    auto mm = memoryManager_->getMemoryManager();
    uint64_t spilledOut = mm->arbitrator()->shrinkCapacity(remaining); // this conducts spill
    uint64_t total = shrunken + spilledOut;
    LOG(INFO) << fmt::format(
        "{} successfully reclaimed total {} with shrunken {} and spilled {}.",
        logPrefix,
        velox::succinctBytes(total),
        velox::succinctBytes(shrunken),
        velox::succinctBytes(spilledOut));
    return total;
  }
  LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy << " was not configured.";
  VLOG(2) << logPrefix << "Successfully reclaimed total " << shrunken << " bytes.";
  return shrunken;
}

void WholeStageResultIterator::getOrderedNodeIds(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    std::vector<velox::core::PlanNodeId>& nodeIds) {
  bool isProjectNode = (std::dynamic_pointer_cast<const velox::core::ProjectNode>(planNode) != nullptr);
  bool isLocalExchangeNode = (std::dynamic_pointer_cast<const velox::core::LocalPartitionNode>(planNode) != nullptr);
  bool isUnionNode = isLocalExchangeNode &&
      std::dynamic_pointer_cast<const velox::core::LocalPartitionNode>(planNode)->type() ==
          velox::core::LocalPartitionNode::Type::kGather;
  const auto& sourceNodes = planNode->sources();
  if (isProjectNode) {
    GLUTEN_CHECK(sourceNodes.size() == 1, "Illegal state");
    const auto sourceNode = sourceNodes.at(0);
    // Filter over Project are mapped into FilterProject operator in Velox.
    // Metrics are all applied on Project node, and the metrics for Filter node
    // do not exist.
    if (std::dynamic_pointer_cast<const velox::core::FilterNode>(sourceNode)) {
      omittedNodeIds_.insert(sourceNode->id());
    }
    getOrderedNodeIds(sourceNode, nodeIds);
    nodeIds.emplace_back(planNode->id());
    return;
  }

  if (isUnionNode) {
    // FIXME: The whole metrics system in gluten-substrait is magic. Passing metrics trees through JNI with a trivial
    //  array is possible but requires for a solid design. Apparently we haven't had it. All the code requires complete
    //  rework.
    // Union was interpreted as LocalPartition + LocalExchange + 2 fake projects as children in Velox. So we only fetch
    // metrics from the root node.
    std::vector<std::shared_ptr<const velox::core::PlanNode>> unionChildren{};
    for (const auto& source : planNode->sources()) {
      const auto projectedChild = std::dynamic_pointer_cast<const velox::core::ProjectNode>(source);
      GLUTEN_CHECK(projectedChild != nullptr, "Illegal state");
      const auto projectSources = projectedChild->sources();
      GLUTEN_CHECK(projectSources.size() == 1, "Illegal state");
      const auto projectSource = projectSources.at(0);
      getOrderedNodeIds(projectSource, nodeIds);
    }
    nodeIds.emplace_back(planNode->id());
    return;
  }

  for (const auto& sourceNode : sourceNodes) {
    // Post-order traversal.
    getOrderedNodeIds(sourceNode, nodeIds);
  }
  nodeIds.emplace_back(planNode->id());
}

void WholeStageResultIterator::constructPartitionColumns(
    std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
    const std::unordered_map<std::string, std::string>& map) {
  for (const auto& partitionColumn : map) {
    auto key = partitionColumn.first;
    const auto value = partitionColumn.second;
    if (!veloxCfg_->get<bool>(kCaseSensitive, false)) {
      folly::toLowerAscii(key);
    }
    if (value == kHiveDefaultPartition) {
      partitionKeys[key] = std::nullopt;
    } else {
      partitionKeys[key] = value;
    }
  }
}

void WholeStageResultIterator::tryAddSplitsToTask() {
  if (noMoreSplits_) {
    return;
  }
  for (int idx = 0; idx < scanNodeIds_.size(); idx++) {
    for (auto& split : splits_[idx]) {
      task_->addSplit(scanNodeIds_[idx], std::move(split));
    }
    task_->noMoreSplits(scanNodeIds_[idx]);
  }
  noMoreSplits_ = true;
}

void WholeStageResultIterator::collectMetrics() {
  if (metrics_) {
    // The metrics has already been created.
    return;
  }

  const auto& taskStats = task_->taskStats();
  if (taskStats.executionStartTimeMs == 0) {
    LOG(INFO) << "Skip collect task metrics since task did not call next().";
    return;
  }

  // Save and print the plan with stats if debug mode is enabled or showTaskMetricsWhenFinished is true.
  if (veloxCfg_->get<bool>(kDebugModeEnabled, false) ||
      veloxCfg_->get<bool>(kShowTaskMetricsWhenFinished, kShowTaskMetricsWhenFinishedDefault)) {
    auto planWithStats = velox::exec::printPlanWithStats(*veloxPlan_.get(), taskStats, true);
    std::ostringstream oss;
    oss << "Native Plan with stats for: " << taskInfo_ << "\n";
    oss << "TaskStats: totalTime: " << taskStats.executionEndTimeMs - taskStats.executionStartTimeMs
        << "; startTime: " << taskStats.executionStartTimeMs << "; endTime: " << taskStats.executionEndTimeMs;
    oss << "\n" << planWithStats << std::endl;
    LOG(WARNING) << oss.str();
  }

  auto planStats = velox::exec::toPlanStats(taskStats);
  // Calculate the total number of metrics.
  int statsNum = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
        LOG(WARNING) << "Not found node id: " << nodeId;
        LOG(WARNING) << "Plan Node: " << std::endl << veloxPlan_->toString(true, true);
        throw std::runtime_error("Node id cannot be found in plan status.");
      }
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      statsNum += 1;
      continue;
    }
    statsNum += planStats.at(nodeId).operatorStats.size();
  }

  metrics_ = std::make_unique<Metrics>(statsNum);

  int metricIndex = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    metrics_->get(Metrics::kLoadLazyVectorTime)[metricIndex] = 0;

    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      metrics_->get(Metrics::kOutputRows)[metricIndex] = 0;
      metrics_->get(Metrics::kOutputVectors)[metricIndex] = 0;
      metrics_->get(Metrics::kOutputBytes)[metricIndex] = 0;
      metrics_->get(Metrics::kCpuCount)[metricIndex] = 0;
      metrics_->get(Metrics::kWallNanos)[metricIndex] = 0;
      metrics_->get(Metrics::kPeakMemoryBytes)[metricIndex] = 0;
      metrics_->get(Metrics::kNumMemoryAllocations)[metricIndex] = 0;
      metricIndex += 1;
      continue;
    }

    const auto& stats = planStats.at(nodeId);
    // Add each operator stats into metrics.
    for (const auto& entry : stats.operatorStats) {
      const auto& second = entry.second;
      metrics_->get(Metrics::kInputRows)[metricIndex] = second->inputRows;
      metrics_->get(Metrics::kInputVectors)[metricIndex] = second->inputVectors;
      metrics_->get(Metrics::kInputBytes)[metricIndex] = second->inputBytes;
      metrics_->get(Metrics::kRawInputRows)[metricIndex] = second->rawInputRows;
      metrics_->get(Metrics::kRawInputBytes)[metricIndex] = second->rawInputBytes;
      metrics_->get(Metrics::kOutputRows)[metricIndex] = second->outputRows;
      metrics_->get(Metrics::kOutputVectors)[metricIndex] = second->outputVectors;
      metrics_->get(Metrics::kOutputBytes)[metricIndex] = second->outputBytes;
      metrics_->get(Metrics::kCpuCount)[metricIndex] = second->cpuWallTiming.count;
      metrics_->get(Metrics::kWallNanos)[metricIndex] = second->cpuWallTiming.wallNanos;
      metrics_->get(Metrics::kPeakMemoryBytes)[metricIndex] = second->peakMemoryBytes;
      metrics_->get(Metrics::kNumMemoryAllocations)[metricIndex] = second->numMemoryAllocations;
      metrics_->get(Metrics::kSpilledInputBytes)[metricIndex] = second->spilledInputBytes;
      metrics_->get(Metrics::kSpilledBytes)[metricIndex] = second->spilledBytes;
      metrics_->get(Metrics::kSpilledRows)[metricIndex] = second->spilledRows;
      metrics_->get(Metrics::kSpilledPartitions)[metricIndex] = second->spilledPartitions;
      metrics_->get(Metrics::kSpilledFiles)[metricIndex] = second->spilledFiles;
      metrics_->get(Metrics::kNumDynamicFiltersProduced)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersProduced);
      metrics_->get(Metrics::kNumDynamicFiltersAccepted)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersAccepted);
      metrics_->get(Metrics::kNumReplacedWithDynamicFilterRows)[metricIndex] =
          runtimeMetric("sum", second->customStats, kReplacedWithDynamicFilterRows);
      metrics_->get(Metrics::kFlushRowCount)[metricIndex] = runtimeMetric("sum", second->customStats, kFlushRowCount);
      metrics_->get(Metrics::kLoadedToValueHook)[metricIndex] =
          runtimeMetric("sum", second->customStats, kLoadedToValueHook);
      metrics_->get(Metrics::kScanTime)[metricIndex] = runtimeMetric("sum", second->customStats, kTotalScanTime);
      metrics_->get(Metrics::kSkippedSplits)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedSplits);
      metrics_->get(Metrics::kProcessedSplits)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedSplits);
      metrics_->get(Metrics::kSkippedStrides)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedStrides);
      metrics_->get(Metrics::kProcessedStrides)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedStrides);
      metrics_->get(Metrics::kRemainingFilterTime)[metricIndex] =
          runtimeMetric("sum", second->customStats, kRemainingFilterTime);
      metrics_->get(Metrics::kIoWaitTime)[metricIndex] = runtimeMetric("sum", second->customStats, kIoWaitTime);
      metrics_->get(Metrics::kStorageReadBytes)[metricIndex] =
          runtimeMetric("sum", second->customStats, kStorageReadBytes);
      metrics_->get(Metrics::kLocalReadBytes)[metricIndex] = runtimeMetric("sum", second->customStats, kLocalReadBytes);
      metrics_->get(Metrics::kRamReadBytes)[metricIndex] = runtimeMetric("sum", second->customStats, kRamReadBytes);
      metrics_->get(Metrics::kPreloadSplits)[metricIndex] =
          runtimeMetric("sum", entry.second->customStats, kPreloadSplits);
      metrics_->get(Metrics::kPageLoadTime)[metricIndex] = runtimeMetric("sum", second->customStats, kPageLoadTime);
      metrics_->get(Metrics::kDataSourceAddSplitWallNanos)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDataSourceAddSplitWallNanos) +
          runtimeMetric("sum", second->customStats, kWaitForPreloadSplitNanos);
      metrics_->get(Metrics::kDataSourceReadWallNanos)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDataSourceReadWallNanos);
      metrics_->get(Metrics::kNumWrittenFiles)[metricIndex] =
          runtimeMetric("sum", entry.second->customStats, kNumWrittenFiles);
      metrics_->get(Metrics::kPhysicalWrittenBytes)[metricIndex] = second->physicalWrittenBytes;
      metrics_->get(Metrics::kWriteIOTime)[metricIndex] = runtimeMetric("sum", second->customStats, kWriteIOTime);

      metricIndex += 1;
    }
  }

  // Put the loadLazyVector time into the metrics of the last operator.
  metrics_->get(Metrics::kLoadLazyVectorTime)[orderedNodeIds_.size() - 1] = loadLazyVectorTime_;

  // Populate the metrics with task stats for long running tasks.
  if (const int64_t collectTaskStatsThreshold =
          veloxCfg_->get<int64_t>(kTaskMetricsToEventLogThreshold, kTaskMetricsToEventLogThresholdDefault);
      collectTaskStatsThreshold >= 0 &&
      static_cast<int64_t>(taskStats.terminationTimeMs - taskStats.executionStartTimeMs) >
          collectTaskStatsThreshold * 1'000) {
    auto jsonStats = velox::exec::toPlanStatsJson(taskStats);
    metrics_->stats = folly::toJson(jsonStats);
  }
}

int64_t WholeStageResultIterator::runtimeMetric(
    const std::string& type,
    const std::unordered_map<std::string, velox::RuntimeMetric>& runtimeStats,
    const std::string& metricId) {
  if (runtimeStats.find(metricId) == runtimeStats.end()) {
    return 0;
  }

  if (type == "sum") {
    return runtimeStats.at(metricId).sum;
  } else if (type == "count") {
    return runtimeStats.at(metricId).count;
  } else if (type == "min") {
    return runtimeStats.at(metricId).min;
  } else if (type == "max") {
    return runtimeStats.at(metricId).max;
  } else {
    return 0;
  }
}

std::unordered_map<std::string, std::string> WholeStageResultIterator::getQueryContextConf() {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set the preferred and max batch size.
  configs[velox::core::QueryConfig::kPreferredOutputBatchRows] =
      std::to_string(veloxCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kMaxOutputBatchRows] =
      std::to_string(veloxCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(veloxCfg_->get<uint64_t>(kVeloxPreferredBatchBytes, 10L << 20));
  try {
    configs[velox::core::QueryConfig::kSparkAnsiEnabled] = veloxCfg_->get<std::string>(kAnsiEnabled, "false");
    configs[velox::core::QueryConfig::kSessionTimezone] = veloxCfg_->get<std::string>(kSessionTimezone, "");
    // Adjust timestamp according to the above configured session timezone.
    configs[velox::core::QueryConfig::kAdjustTimestampToTimezone] = "true";

    {
      // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
      // Partial aggregation memory configurations.
      // TODO: Move the calculations to Java side.
      auto offHeapMemory = veloxCfg_->get<int64_t>(kSparkTaskOffHeapMemory, facebook::velox::memory::kMaxMemory);
      auto maxPartialAggregationMemory = std::max<int64_t>(
          1 << 24,
          veloxCfg_->get<int64_t>(kMaxPartialAggregationMemory).has_value()
              ? veloxCfg_->get<int64_t>(kMaxPartialAggregationMemory).value()
              : static_cast<int64_t>(veloxCfg_->get<double>(kMaxPartialAggregationMemoryRatio, 0.1) * offHeapMemory));
      auto maxExtendedPartialAggregationMemory = std::max<int64_t>(
          1 << 26,
          static_cast<long>(veloxCfg_->get<double>(kMaxExtendedPartialAggregationMemoryRatio, 0.15) * offHeapMemory));
      configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxPartialAggregationMemory);
      configs[velox::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
          std::to_string(maxExtendedPartialAggregationMemory);
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinPct] =
          std::to_string(veloxCfg_->get<int32_t>(kAbandonPartialAggregationMinPct, 90));
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinRows] =
          std::to_string(veloxCfg_->get<int32_t>(kAbandonPartialAggregationMinRows, 100000));
    }
    // Spill configs
    if (spillStrategy_ == "none") {
      configs[velox::core::QueryConfig::kSpillEnabled] = "false";
    } else {
      configs[velox::core::QueryConfig::kSpillEnabled] = "true";
    }
    configs[velox::core::QueryConfig::kAggregationSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kAggregationSpillEnabled, true));
    configs[velox::core::QueryConfig::kJoinSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kJoinSpillEnabled, true));
    configs[velox::core::QueryConfig::kOrderBySpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kOrderBySpillEnabled, true));
    configs[velox::core::QueryConfig::kWindowSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kWindowSpillEnabled, true));
    configs[velox::core::QueryConfig::kMaxSpillLevel] = std::to_string(veloxCfg_->get<int32_t>(kMaxSpillLevel, 4));
    configs[velox::core::QueryConfig::kMaxSpillFileSize] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillFileSize, 1L * 1024 * 1024 * 1024));
    configs[velox::core::QueryConfig::kMaxSpillRunRows] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillRunRows, 3L * 1024 * 1024));
    configs[velox::core::QueryConfig::kMaxSpillBytes] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillBytes, 107374182400LL));
    configs[velox::core::QueryConfig::kSpillWriteBufferSize] =
        std::to_string(veloxCfg_->get<uint64_t>(kShuffleSpillDiskWriteBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillReadBufferSize] =
        std::to_string(veloxCfg_->get<int32_t>(kSpillReadBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillStartPartitionBit] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillStartPartitionBit, 48));
    configs[velox::core::QueryConfig::kSpillNumPartitionBits] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillPartitionBits, 3));
    configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillableReservationGrowthPct, 25));
    configs[velox::core::QueryConfig::kSpillPrefixSortEnabled] =
        veloxCfg_->get<std::string>(kSpillPrefixSortEnabled, "false");
    if (veloxCfg_->get<bool>(kSparkShuffleSpillCompress, true)) {
      configs[velox::core::QueryConfig::kSpillCompressionKind] =
          veloxCfg_->get<std::string>(kSpillCompressionKind, veloxCfg_->get<std::string>(kCompressionKind, "lz4"));
    } else {
      configs[velox::core::QueryConfig::kSpillCompressionKind] = "none";
    }
    configs[velox::core::QueryConfig::kSparkBloomFilterExpectedNumItems] =
        std::to_string(veloxCfg_->get<int64_t>(kBloomFilterExpectedNumItems, 1000000));
    configs[velox::core::QueryConfig::kSparkBloomFilterNumBits] =
        std::to_string(veloxCfg_->get<int64_t>(kBloomFilterNumBits, 8388608));
    configs[velox::core::QueryConfig::kSparkBloomFilterMaxNumBits] =
        std::to_string(veloxCfg_->get<int64_t>(kBloomFilterMaxNumBits, 4194304));
    // spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver takes no effect if
    // spark.gluten.sql.columnar.backend.velox.IOThreads is set to 0
    configs[velox::core::QueryConfig::kMaxSplitPreloadPerDriver] =
        std::to_string(veloxCfg_->get<int32_t>(kVeloxSplitPreloadPerDriver, 2));

    // hashtable build optimizations
    configs[velox::core::QueryConfig::kAbandonBuildNoDupHashMinRows] =
        std::to_string(veloxCfg_->get<int32_t>(kAbandonBuildNoDupHashMinRows, 100000));
    configs[velox::core::QueryConfig::kAbandonBuildNoDupHashMinPct] =
        std::to_string(veloxCfg_->get<int32_t>(kAbandonBuildNoDupHashMinPct, 0));

    // Disable driver cpu time slicing.
    configs[velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs] = "0";

    configs[velox::core::QueryConfig::kSparkPartitionId] = std::to_string(taskInfo_.partitionId);

    // Enable Spark legacy date formatter if spark.sql.legacy.timeParserPolicy is set to 'LEGACY'
    // or 'legacy'
    if (veloxCfg_->get<std::string>(kSparkLegacyTimeParserPolicy, "") == "LEGACY") {
      configs[velox::core::QueryConfig::kSparkLegacyDateFormatter] = "true";
    } else {
      configs[velox::core::QueryConfig::kSparkLegacyDateFormatter] = "false";
    }

    if (veloxCfg_->get<std::string>(kSparkMapKeyDedupPolicy, "") == "EXCEPTION") {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "true";
    } else {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "false";
    }

    configs[velox::core::QueryConfig::kSparkLegacyStatisticalAggregate] =
        std::to_string(veloxCfg_->get<bool>(kSparkLegacyStatisticalAggregate, false));

    configs[velox::core::QueryConfig::kSparkJsonIgnoreNullFields] =
        std::to_string(veloxCfg_->get<bool>(kSparkJsonIgnoreNullFields, true));

    configs[velox::core::QueryConfig::kExprMaxCompiledRegexes] =
        std::to_string(veloxCfg_->get<int32_t>(kExprMaxCompiledRegexes, 100));

#ifdef GLUTEN_ENABLE_GPU
    configs[velox::cudf_velox::CudfConfig::kCudfEnabled] = std::to_string(veloxCfg_->get<bool>(kCudfEnabled, false));
#endif

    const auto setIfExists = [&](const std::string& glutenKey, const std::string& veloxKey) {
      const auto valueOptional = veloxCfg_->get<std::string>(glutenKey);
      if (valueOptional.has_value()) {
        configs[veloxKey] = valueOptional.value();
      }
    };
    setIfExists(kQueryTraceEnabled, velox::core::QueryConfig::kQueryTraceEnabled);
    setIfExists(kQueryTraceDir, velox::core::QueryConfig::kQueryTraceDir);
    setIfExists(kQueryTraceMaxBytes, velox::core::QueryConfig::kQueryTraceMaxBytes);
    setIfExists(kQueryTraceTaskRegExp, velox::core::QueryConfig::kQueryTraceTaskRegExp);
    setIfExists(kOpTraceDirectoryCreateConfig, velox::core::QueryConfig::kOpTraceDirectoryCreateConfig);
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

std::shared_ptr<velox::config::ConfigBase> WholeStageResultIterator::createConnectorConfig() {
  // The configs below are used at session level.
  std::unordered_map<std::string, std::string> configs = {};
  // The semantics of reading as lower case is opposite with case-sensitive.
  configs[velox::connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession] =
      !veloxCfg_->get<bool>(kCaseSensitive, false) ? "true" : "false";
  configs[velox::connector::hive::HiveConfig::kPartitionPathAsLowerCaseSession] = "false";
  configs[velox::parquet::WriterOptions::kParquetSessionWriteTimestampUnit] = "6";
  configs[velox::connector::hive::HiveConfig::kReadTimestampUnitSession] = "6";
  configs[velox::connector::hive::HiveConfig::kMaxPartitionsPerWritersSession] =
      std::to_string(veloxCfg_->get<int32_t>(kMaxPartitions, 10000));
  configs[velox::connector::hive::HiveConfig::kIgnoreMissingFilesSession] =
      std::to_string(veloxCfg_->get<bool>(kIgnoreMissingFiles, false));
  configs[velox::connector::hive::HiveConfig::kParquetUseColumnNamesSession] =
      std::to_string(veloxCfg_->get<bool>(kParquetUseColumnNames, true));
  configs[velox::connector::hive::HiveConfig::kOrcUseColumnNamesSession] =
      std::to_string(veloxCfg_->get<bool>(kOrcUseColumnNames, true));
  return std::make_shared<velox::config::ConfigBase>(std::move(configs));
}

} // namespace gluten
