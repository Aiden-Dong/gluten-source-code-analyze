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

#include "compute/Runtime.h"
#include "iceberg/IcebergPlanConverter.h"
#include "memory/ColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "utils/Metrics.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#ifdef GLUTEN_ENABLE_GPU
#include "cudf/GpuLock.h"
#endif

namespace gluten {

/*****
 *  1. 整阶段执行: 将整个Spark Stage转换为单个Velox任务执行
 *  2. 内存管理: 管理列式数据的内存分配和释放
 *  3. 数据流控制: 控制数据分片的加载和处理流程
 *  4. 性能监控: 收集详细的执行指标用于优化
 *  5. 资源清理: 确保任务取消和资源正确释放
 *  简单理解: 这是Gluten中负责执行Velox计划并返回结果的核心迭代器，类似于Spark的执行引擎。
 */
class WholeStageResultIterator : public ColumnarBatchIterator {
 public:

  // // 构造函数：初始化执行环境
  WholeStageResultIterator(
      VeloxMemoryManager* memoryManager,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,  // Velox 计划树
      const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const facebook::velox::config::ConfigBase* veloxCfg,
      const SparkTaskInfo& taskInfo);

  // // 析构函数：清理资源和取消任务
  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
#ifdef GLUTEN_ENABLE_GPU
    if (enableCudf_) {
      unlockGpu();
    }
#endif
  }

  // // 获取下一批数据
  std::shared_ptr<ColumnarBatch> next() override;

  // 溢写固定大小数据
  int64_t spillFixedSize(int64_t size) override;

  Metrics* getMetrics(int64_t exportNanos) {
    collectMetrics();
    if (metrics_) {
      metrics_->veloxToArrow = exportNanos;
    }
    return metrics_.get();
  }

  const facebook::velox::exec::Task* task() const {
    return task_.get();
  }

  const facebook::velox::core::PlanNode* veloxPlan() const {
    return veloxPlan_.get();
  }

 private:
  /// Get the Spark confs to Velox query context.
  /// // 获取查询上下文配置
  std::unordered_map<std::string, std::string> getQueryContextConf();

  /// // 创建Velox查询上下文.
  std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx();

  /// 获取有序节点ID.
  void getOrderedNodeIds(
      const std::shared_ptr<const facebook::velox::core::PlanNode>&,
      std::vector<facebook::velox::core::PlanNodeId>& nodeIds);

  /// 创建连接器配置.
  std::shared_ptr<facebook::velox::config::ConfigBase> createConnectorConfig();

  /// 构造分区列.
  void constructPartitionColumns(
      std::unordered_map<std::string, std::optional<std::string>>&,
      const std::unordered_map<std::string, std::string>&);

  /// 向任务添加数据分片.
  void tryAddSplitsToTask();

  /// Collect Velox metrics.
  void collectMetrics();

  /// Return a certain type of runtime metric. Supported metric types are: sum, count, min, max.
  static int64_t runtimeMetric(
      const std::string& type,
      const std::unordered_map<std::string, facebook::velox::RuntimeMetric>& runtimeStats,
      const std::string& metricId);

  /// Memory.
  VeloxMemoryManager* memoryManager_;            // 内存管理器

  /// Config, task and plan.
  const config::ConfigBase* veloxCfg_;                                 // Velox配置

  #ifdef GLUTEN_ENABLE_GPU
  const bool enableCudf_;
#endif


  const SparkTaskInfo taskInfo_;                                       // Spark任务信息
  std::shared_ptr<facebook::velox::exec::Task> task_;                  // Velox执行任务
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;   // Velox执行计划

  /// Spill.
  std::string spillStrategy_;                                          // 溢写策略
  std::shared_ptr<folly::Executor> spillExecutor_ = nullptr;           // 溢写执行器

  /// Metrics
  std::unique_ptr<Metrics> metrics_{};                                 // // 性能指标

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;     // // 有序节点ID

  /// Node ids should be omitted in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;  // 忽略的节点ID
  std::vector<facebook::velox::core::PlanNodeId> scanNodeIds_;            // 扫描节点ID列表
  std::vector<std::shared_ptr<SplitInfo>> scanInfos_;                     // 数据分片信息
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;              // 流节点ID列表
  std::vector<std::vector<facebook::velox::exec::Split>> splits_;         // 数据分片
  bool noMoreSplits_ = false;                                             // 是否还有更多分片

  int64_t loadLazyVectorTime_ = 0;                                        // // 延迟向量加载时间
};

} // namespace gluten
