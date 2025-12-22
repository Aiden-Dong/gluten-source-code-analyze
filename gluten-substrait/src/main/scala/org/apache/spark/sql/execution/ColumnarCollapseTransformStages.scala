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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

/**
 * A bridge to connect [[SparkPlan]] and [[TransformSupport]] and provide the substrait plan
 * `ReadRel` for the child columnar iterator, so that the [[TransformSupport]] always has input. It
 * would be transformed to `ValueStreamNode` at native side.
 */
case class InputIteratorTransformer(child: SparkPlan) extends UnaryTransformSupport {

  @transient
  override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genInputIteratorTransformerMetrics(
      child,
      sparkContext,
      forBroadcast(),
      forShuffle())

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance
      .genInputIteratorTransformerMetricsUpdater(metrics, forBroadcast())

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    assert(child.isInstanceOf[ColumnarInputAdapter])
    child.doExecuteBroadcast()
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    assert(child.isInstanceOf[ColumnarInputAdapter])
    val operatorId = context.nextOperatorId(nodeName)
    val readRel = RelBuilder.makeReadRelForInputIterator(child.output.asJava, context, operatorId)
    TransformContext(output, readRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  private def forBroadcast(): Boolean = {
    child match {
      case ColumnarInputAdapter(c)
          if c.isInstanceOf[BroadcastQueryStageExec] ||
            c.isInstanceOf[BroadcastExchangeLike] =>
        true
      case _ => false
    }
  }

  private def forShuffle(): Boolean = {
    child match {
      case ColumnarInputAdapter(c)
          if c.isInstanceOf[ShuffleQueryStageExec] || c.isInstanceOf[ShuffleExchangeLike] =>
        true
      case _ => false
    }
  }
}

/**
 * 参考Spark的CollapseCodegenStages实现。
 *
 * 查找支持transform的链式计划，将它们合并为WholeStageTransformer。
 *
 * `transformStageCounter`为查询计划中的transform阶段生成ID。它不影响相等性比较，也不参与WholeStageTransformer的解构模式匹配。
 *
 * 这个ID用于帮助区分不同的transform阶段。它作为物理计划explain输出的一部分，例如：
 *
 * ==Physical Plan==
 * *(5) SortMergeJoin [x#3L], [y#9L], Inner :- *(2) Sort [x#3L ASC NULLS FIRST], false, 0 : +-
 * Exchange hashpartitioning(x#3L, 200) : +- *(1) Project [(id#0L % 2) AS x#3L] : +- *(1) Filter
 * isnotnull((id#0L % 2)) : +- *(1) Range (0, 5, step=1, splits=8) +- *(4) Sort [y#9L ASC NULLS
 * FIRST], false, 0 +- Exchange hashpartitioning(y#9L, 200) +- *(3) Project [(id#6L % 2) AS y#9L] +-
 * *(3) Filter isnotnull((id#6L % 2)) +- *(3) Range (0, 5, step=1, splits=8)
 *
 * 其中ID清楚地表明，并非所有相邻的转换计划操作符都属于同一个transform阶段。
 *
 * transform阶段ID也可选地包含在生成类的名称中作为后缀，这样更容易将生成的类
 * 关联回物理操作符。这由SQLConf控制：spark.sql.codegen.useIdInClassName
 *
 * ID也包含在各种日志消息中。
 *
 * 在查询中，计划中的transform阶段从1开始按"插入顺序"计数。 WholeStageTransformer操作符按深度优先后序插入到计划中。
 * 插入顺序的定义请参见CollapseTransformStages.insertWholeStageTransform。
 *
 * 0被保留作为特殊ID值，表示创建了临时的WholeStageTransformer对象， 例如，当现有WholeStageTransformer无法生成/编译代码时的特殊回退处理。
 *
 * FilterExecTransformer (支持Transform) └─ ProjectExecTransformer (支持Transform) └─
 * ShuffleExchangeExec (不支持Transform) └─ ScanTransformer (支持Transform)
 *
 * --------------------------------------------- WholeStageTransformer(stageId=1) └─
 * FilterExecTransformer └─ ProjectExecTransformer └─ InputIteratorTransformer └─
 * ColumnarInputAdapter └─ ShuffleExchangeExec └─ WholeStageTransformer(stageId=2) └─
 * ScanTransformer
 */
case class ColumnarCollapseTransformStages(
    glutenConf: GlutenConfig,
    transformStageCounter: AtomicInteger = ColumnarCollapseTransformStages.transformStageCounter)
  extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    insertWholeStageTransformer(plan)
  }

  /**
   * When it's the ClickHouse backend, BasicScanExecTransformer will not be included in
   * WholeStageTransformer.
   */
  private def isSeparateBaseScanExecTransformer(plan: SparkPlan): Boolean = plan match {
    case _: BasicScanExecTransformer =>
      BackendsApiManager.getSettings.excludeScanExecFromCollapsedStage()
    case _ => false
  }

  private def supportTransform(plan: SparkPlan): Boolean = plan match {
    case plan: TransformSupport if !isSeparateBaseScanExecTransformer(plan) => true
    case _ => false
  }

  /** Inserts an InputIteratorTransformer on top of those that do not support transform. */
  private def insertInputIteratorTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      // 遍历子节点找到不支持Transform节点
      case p if !supportTransform(p) =>
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(insertWholeStageTransformer(p))
      case p =>
        p.withNewChildren(p.children.map(insertInputIteratorTransformer))
    }
  }

  private def insertWholeStageTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      // 当前节点支持 Transform
      case t if supportTransform(t) =>
        WholeStageTransformer(t.withNewChildren(t.children.map(insertInputIteratorTransformer)))(
          transformStageCounter.incrementAndGet())
      // 该节点不支持 Transform
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageTransformer))
    }
  }
}

// TODO: Make this inherit from GlutenPlan.
case class ColumnarInputAdapter(child: SparkPlan)
  extends InputAdapterGenerateTreeStringShim
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33OrLater
  with GlutenPlan.SupportsRowBasedCompatible
  with ConventionReq.KnownChildConvention {
  override def output: Seq[Attribute] = child.output
  final override val supportsColumnar: Boolean = true
  final override val supportsRowBased: Boolean = false
  override def rowType0(): Convention.RowType = Convention.RowType.None
  override def batchType(): Convention.BatchType =
    BackendsApiManager.getSettings.primaryBatchType
  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(
      ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = child.executeColumnar()
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = child.doExecuteBroadcast()
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
  // Node name's required to be "InputAdapter" to correctly draw UI graph.
  // See buildSparkPlanGraphNode in SparkPlanGraph.scala of Spark.
  override def nodeName: String = s"InputAdapter"
}

object ColumnarCollapseTransformStages {
  val transformStageCounter = new AtomicInteger(0)

  def wrapInputIteratorTransformer(plan: SparkPlan): TransformSupport = {
    InputIteratorTransformer(ColumnarInputAdapter(plan))
  }
}
