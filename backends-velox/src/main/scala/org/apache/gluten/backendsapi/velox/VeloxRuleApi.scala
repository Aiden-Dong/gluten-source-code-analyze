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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.{BackendsApiManager, RuleApi}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{PreventBatchTypeMismatchInTableCache, RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.V2WritePostRule
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.noop.GlutenNoopWriterRule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer


/****
 * 规则注入
 */
class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)    // Legacy 是成熟稳定的优化路径，提供可靠的性能提升
    injectRas(injector.gluten.ras)          // 是下一代智能优化系统，提供更精细的控制和更好的优化潜力，但需要显式启用且可能存在更多的实验性特性。
  }
}

object VeloxRuleApi {

  /**
   * 注册Spark规则或扩展，但不包括Gluten的列式规则，这些列式规则应该通过 [[injectLegacy]] / [[injectRas]] 方法注入。
   */
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectOptimizerRule(CollectRewriteRule.apply)                        // 集合函数重写
    injector.injectOptimizerRule(HLLRewriteRule.apply)                            // HyperLogLog重写
    injector.injectOptimizerRule(CollapseGetJsonObjectExpressionRule.apply)       // 折叠和优化JSON对象访问表达式
    injector.injectOptimizerRule(RewriteCastFromArray.apply)                      // 数组类型转换重写
    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)                // Arrow转换规则
    injector.injectOptimizerRule(RewriteUnboundedWindow.apply)                    // 无界窗口重写

    if (BackendsApiManager.getSettings.supportAppendDataExec()) {
      injector.injectPlannerStrategy(SparkShimLoader.getSparkShims.getRewriteCreateTableAsSelect(_))  // 重写CREATE TABLE AS SELECT语句
    }
  }

  /**
   * 注册Gluten的列式规则。这些规则将在Gluten中默认执行，用于列式查询规划
   */
  private def injectLegacy(injector: LegacyInjector): Unit = {
    // TODO-1 : 预转换规则 rules.
    injector.injectPreTransform(_ => RemoveTransitions)                            // 移除现有的行列转换
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)       // 文件表达式下推
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))          // ANSI模式回退检查
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))       // 多代码生成回退
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))   // 合并两阶段哈希聚合
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())                   // 重写子查询广播

    // 布隆过滤器JOIN重写
    injector.injectPreTransform(
      c =>
        BloomFilterMightContainJointRewriteRule.apply(
          c.session,
          c.caller.isBloomFilterStatFunction()))

    // Arrow扫描替换
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // 消除冗余时间戳获取
    injector.injectPreTransform(_ => EliminateRedundantGetTimestamp)

    // TODO-2 : 核心转换规则.
    // 算子卸载规则
    val offloads = Seq(OffloadOthers(),     // 其他算子卸载
      OffloadExchange(),                    // Exchange算子卸载
      OffloadJoin()                         // Join算子卸载
    ).map(_.toStrcitRule())

    val validatorBuilder: GlutenConfig => Validator = conf =>
      Validators.newValidator(conf, offloads)      // 验证器构建


    val rewrites =
      Seq(
        RewriteIn,                       // IN表达式重写
        RewriteMultiChildrenCount,       // 多子节点计数重写
        RewriteJoin,                     // Join重写
        PullOutPreProject,               // 提取前置投影
        PullOutPostProject,              //  提取后置投影
        ProjectColumnPruning)            // 投影列裁剪

    // 启发式转换-核心功能
    injector.injectTransform(
      c =>
        HeuristicTransform.WithRewrites(
          validatorBuilder(new GlutenConfig(c.sqlConf)),
          rewrites,
          offloads))

    // TODO-3 : 后转换规则.
    injector.injectPostTransform(_ => AppendBatchResizeForShuffleInputAndOutput())    // Shuffle输入输出批次调整
    injector.injectPostTransform(_ => GpuBufferBatchResizeForShuffleInputOutput())
    injector.injectPostTransform(_ => UnionTransformerRule())                         // Union转换器规则
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))            // 部分投影和生成规则
    injector.injectPostTransform(_ => PartialGenerateRule())
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())         // 移除原生写文件排序和投影
    injector.injectPostTransform(_ => PushDownFilterToScan)                           // 过滤器下推到扫描
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)        // 文件表达式下推
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)                    // 排序优化
    injector.injectPostTransform(_ => EliminateLocalSort)                             // 投影优化
    injector.injectPostTransform(_ => PullOutDuplicateProject)                        // 聚合优化
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)                 // Collect操作优化
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(_ => CollectLimitTransformerRule())
    injector.injectPostTransform(_ => CollectTailTransformerRule())
    injector.injectPostTransform(_ => V2WritePostRule())                              // V2写入后处理

    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatchType)) // 插入行列转换 (重要)

    // TODO-4 : 回退策略
    injector.injectFallbackPolicy(c => p => ExpandFallbackPolicy(c.caller.isAqe(), p))    // 扩展回退策略

    // TODO-5 : 后规则处理.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.caller.isAqe()))       // 移除最顶层的列式到行转换

    SparkShimLoader.getSparkShims                                                           // 扩展列式后处理规则
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))

    injector.injectPost(c => ColumnarCollapseTransformStages(new GlutenConfig(c.sqlConf)))   // 列式折叠转换阶段
    injector.injectPost(c => CudfNodeValidationRule(new GlutenConfig(c.sqlConf)))            // CUDF节点验证

    injector.injectPost(c => GlutenNoopWriterRule(c.session))                                // Gluten空操作写入器

    // TODO-6 : 最终规则
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))                                 // 移除Gluten表缓存列式到行转换
    injector.injectFinal(c => PreventBatchTypeMismatchInTableCache(c.caller.isCache(), Set(VeloxBatchType)))  // 防止表缓存中的批次类型不匹配
    injector.injectFinal(c => GlutenAutoAdjustStageResourceProfile(new GlutenConfig(c.sqlConf), c.session))   // 自动调整Stage资源配置
    injector.injectFinal(c => GlutenFallbackReporter(new GlutenConfig(c.sqlConf), c.session))                 // Gluten回退报告器
    injector.injectFinal(_ => RemoveFallbackTagRule())                                                        // 移除回退标签
  }

  /**
   * 注册Gluten的列式规则。这些规则仅在通过spark.gluten.ras.enabled=true启用RAS
   * (关系代数选择器)时才会执行。
   *
   * 这些规则由CI测试作业spark-test-spark35-ras覆盖。
   */
  private def injectRas(injector: RasInjector): Unit = {
    // TODO-1 : 移除转换操作.
    injector.injectPreTransform(_ => RemoveTransitions)                           // 移除转换操作
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)      // 下推输入文件表达式
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))         // ANSI 模式回退处理
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))  // 合并两阶段哈希聚合
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())                  // 重写子查询广播
    injector.injectPreTransform(                                                  // 布隆过滤器重写
      c =>
        BloomFilterMightContainJointRewriteRule.apply(
          c.session,
          c.caller.isBloomFilterStatFunction()))
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))      // Arrow 扫描替换
    injector.injectPreTransform(_ => EliminateRedundantGetTimestamp)             // 消除冗余时间戳获取


    // Gluten RAS: The RAS rule.
    val validatorBuilder: GlutenConfig => Validator = conf => Validators.newValidator(conf)  // 验证器构建

    val rewrites =
      Seq(
        RewriteIn,                        // IN 表达式重写
        RewriteMultiChildrenCount,        // 多子节点计数重写
        RewriteJoin,                      // Join 重写
        PullOutPreProject,                // 提取前置投影
        PullOutPostProject,               // 提取后置投影
        ProjectColumnPruning)             // 投影列裁剪

    // 卸载规则集合（支持的算子类型）
    val offloads: Seq[RasOffload] = Seq(
      RasOffload.from[Exchange](OffloadExchange()),               // 数据交换
      RasOffload.from[BaseJoinExec](OffloadJoin()),               // Join 操作
      RasOffload.from[FilterExec](OffloadOthers()),               // 过滤
      RasOffload.from[ProjectExec](OffloadOthers()),              // 投影
      RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()), // 数据源扫描
      RasOffload.from[DataSourceScanExec](OffloadOthers()),       // 哈希聚合
      RasOffload.from(HiveTableScanExecTransformer.isHiveTableScan(_))(OffloadOthers()),  // 排序
      RasOffload.from[CoalesceExec](OffloadOthers()),             // 窗口函数
      RasOffload.from[HashAggregateExec](OffloadOthers()),
      RasOffload.from[SortAggregateExec](OffloadOthers()),
      RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
      RasOffload.from[UnionExec](OffloadOthers()),
      RasOffload.from[ExpandExec](OffloadOthers()),
      RasOffload.from[WriteFilesExec](OffloadOthers()),
      RasOffload.from[SortExec](OffloadOthers()),
      RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
      RasOffload.from[WindowExec](OffloadOthers()),
      RasOffload.from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec(_))(OffloadOthers()),
      RasOffload.from[LimitExec](OffloadOthers()),
      RasOffload.from[GenerateExec](OffloadOthers()),
      RasOffload.from[EvalPythonExec](OffloadOthers()),
      RasOffload.from[SampleExec](OffloadOthers()),
      RasOffload.from[CollectLimitExec](OffloadOthers()),
      RasOffload.from[RangeExec](OffloadOthers())
    )

    offloads.foreach(
      offload =>
        injector.injectRasRule(
          c => RasOffload.Rule(offload, validatorBuilder(new GlutenConfig(c.sqlConf)), rewrites)))

    // TODO-3 : 后规则处理.
    injector.injectPostTransform(_ => DistinguishIdenticalScans)                     // 区分相同扫描
    injector.injectPostTransform(_ => AppendBatchResizeForShuffleInputAndOutput())   // Shuffle 输入输出批次调整
    injector.injectPostTransform(_ => GpuBufferBatchResizeForShuffleInputOutput())   // GPU 缓冲区批次调整
    injector.injectPostTransform(_ => RemoveTransitions)                             // 移除转换
    injector.injectPostTransform(_ => UnionTransformerRule())                        // Union 转换规则
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))           // 部分投影规则
    injector.injectPostTransform(_ => PartialGenerateRule())
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => PullOutDuplicateProject)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(_ => CollectLimitTransformerRule())
    injector.injectPostTransform(_ => CollectTailTransformerRule())
    injector.injectPostTransform(_ => V2WritePostRule())
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatchType))  // 插入转换
    injector.injectPostTransform(c => RemoveTopmostColumnarToRow(c.session, c.caller.isAqe()))  // 移除最顶层的列式到行式转
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPostTransform(c => each(c.session)))
    injector.injectPostTransform(c => ColumnarCollapseTransformStages(new GlutenConfig(c.sqlConf)))
    injector.injectPostTransform(c => CudfNodeValidationRule(new GlutenConfig(c.sqlConf)))
    injector.injectPostTransform(c => GlutenNoopWriterRule(c.session))
    injector.injectPostTransform(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectPostTransform(
      c => PreventBatchTypeMismatchInTableCache(c.caller.isCache(), Set(VeloxBatchType)))
    injector.injectPostTransform(
      c => GlutenAutoAdjustStageResourceProfile(new GlutenConfig(c.sqlConf), c.session))   // 资源配置自动调整
    injector.injectPostTransform(
      c => GlutenFallbackReporter(new GlutenConfig(c.sqlConf), c.session))
    injector.injectPostTransform(_ => RemoveFallbackTagRule())                    // 回退报告
  }
}
