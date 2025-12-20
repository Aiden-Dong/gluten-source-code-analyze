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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression._
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.{GlutenTimeMetric, MetricsUpdater}
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.{LocalFilesNode, RelNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import org.apache.hadoop.fs.viewfs.ViewFileSystemUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

case class TransformContext(outputAttributes: Seq[Attribute], root: RelNode)

case class WholeStageTransformContext(
    root: PlanNode,
    substraitContext: SubstraitContext = null,
    enableCudf: Boolean = false)

/** 可以解释为 Substrait 表示的查询计划基础接口。 */
trait TransformSupport extends ValidatablePlan {

  override def batchType(): Convention.BatchType = {
    BackendsApiManager.getSettings.primaryBatchType
  }

  override def rowType0(): Convention.RowType = {
    Convention.RowType.None
  }

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  def isCudf: Boolean = getTagValue[Boolean](CudfTag.CudfTag).getOrElse(false)

  // Use super.nodeName will cause exception scala 213 Super calls can only target methods
  // for FileSourceScan.
  override def nodeName: String =
    if (isCudf) {
      "Cudf" + getClass.getSimpleName.replaceAll("Exec$", "")
    } else getClass.getSimpleName

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  // Since https://github.com/apache/incubator-gluten/pull/2185.
  protected def doNativeValidation(context: SubstraitContext, node: RelNode): ValidationResult = {
    if (node != null && enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(node))
      BackendsApiManager.getValidatorApiInstance
        .doNativeValidateWithFailureReason(planNode)
    } else {
      ValidationResult.succeeded
    }
  }

  final def transform(context: SubstraitContext): TransformContext = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException(
        "A canonicalized plan is not supposed to be executed transform.")
    }
    if (TransformerState.underValidationState) {
      doTransform(context)
    } else {
      // Materialize subquery first before going to do transform.
      executeQuery {
        doTransform(context)
      }
    }
  }

  protected def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def metricsUpdater(): MetricsUpdater

  protected def getColumnarInputRDDs(plan: SparkPlan): Seq[RDD[ColumnarBatch]] = {
    plan match {
      case c: TransformSupport =>
        c.columnarInputRDDs
      case _ =>
        Seq(plan.executeColumnar())
    }
  }

  // When true, it will not generate relNode, nor will it generate native metrics.
  def isNoop: Boolean = false
}

trait LeafTransformSupport extends TransformSupport with LeafExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = Seq.empty

  /** Returns the split infos that will be processed by the underlying native engine. */
  def getSplitInfos: Seq[SplitInfo]

  /** Returns the partitions generated by this data source scan. */
  def getPartitions: Seq[Partition]

  /** Returns the partitions generated by this data source scan and tied with ReadFileFormat. */
  def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)]
}

trait UnaryTransformSupport extends TransformSupport with UnaryExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(child)
  }
}

case class WholeStageTransformer(child: SparkPlan, materializeInput: Boolean = false)(
    val transformStageId: Int
) extends WholeStageTransformerGenerateTreeStringShim
  with UnaryTransformSupport {

  def stageId: Int = transformStageId

  def wholeStageTransformerContextDefined: Boolean = wholeStageTransformerContext.isDefined

  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genWholeStageTransformerMetrics(sparkContext)

  @transient
  private var wholeStageTransformerContext: Option[WholeStageTransformContext] = None

  // 当前物理计划输出的 Schema 序列化信息
  private var outputSchemaForPlan: Option[TypeNode] = None

  private def inferSchemaFromAttributes(attrs: Seq[Attribute]): TypeNode = {
    val outputTypeNodeList = new java.util.ArrayList[TypeNode]()
    for (attr <- attrs) {
      outputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
    }

    TypeBuilder.makeStruct(false, outputTypeNodeList)
  }

  def setOutputSchemaForPlan(expectOutput: Seq[Attribute]): Unit = {
    if (outputSchemaForPlan.isDefined) {
      return
    }

    // Fixes issue-1874
    outputSchemaForPlan = Some(inferSchemaFromAttributes(expectOutput))
  }

  // 将当前的计划树转为 SubstraitPlan
  def substraitPlan: PlanNode = {
    if (wholeStageTransformerContext.isDefined) {
      wholeStageTransformerContext.get.root
    } else {
      generateWholeStageTransformContext().root
    }
  }

  // 将当前的计划树，转为 ProtoBuf-Json 格式
  def substraitPlanJson: String = {
    SubstraitPlanPrinterUtil.substraitPlanToJson(substraitPlan.toProtobuf)
  }

  def nativePlanString(details: Boolean = true): String = {
    BackendsApiManager.getTransformerApiInstance.getNativePlanString(
      substraitPlan.toProtobuf.toByteArray,
      details)
  }

  // 输出Schema 信心
  override def output: Seq[Attribute] = child.output

  // 输出的分区信息
  override def outputPartitioning: Partitioning = child.outputPartitioning

  // 分区的排序信息
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  // 当前节点名称
  // It's misleading with "Codegen" used. But we have to keep "WholeStageCodegen" prefixed to
  // make whole stage transformer clearly plotted in UI, like spark's whole stage codegen.
  // See buildSparkPlanGraphNode in SparkPlanGraph.scala of Spark.
  override def nodeName: String = s"WholeStageCodegenTransformer ($transformStageId)"

  override def verboseStringWithOperatorId(): String = {
    val nativePlan = if (glutenConf.getConf(GlutenConfig.INJECT_NATIVE_PLAN_STRING_TO_EXPLAIN)) {
      s"Native Plan:\n${nativePlanString()}"
    } else {
      ""
    }
    super.verboseStringWithOperatorId() ++ nativePlan
  }

  private def generateWholeStageTransformContext(): WholeStageTransformContext = {

    // 创建Substrait上下文，用于管理整个转换过程中的元数据和状态
    val substraitContext = new SubstraitContext

    // 对子节点执行transform操作，将Spark物理计划转换为Substrait表示
    val childCtx:TransformContext = child
      .asInstanceOf[TransformSupport]
      .transform(substraitContext)

    if (childCtx == null) {
      throw new IllegalStateException(s"WholeStageTransformer can't do Transform on $child")
    }

    // 转换当前的 outputAttributes
    val outNames = childCtx.outputAttributes.map(ConverterUtils.genColumnNameWithExprId).asJava

    val planNode = if (BackendsApiManager.getSettings.needOutputSchemaForPlan()) {
      val outputSchema =
        outputSchemaForPlan.getOrElse(inferSchemaFromAttributes(childCtx.outputAttributes))

      PlanBuilder.makePlan(
        substraitContext,
        Lists.newArrayList(childCtx.root),
        outNames,
        outputSchema,
        null)
    } else {
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)
    }

    WholeStageTransformContext(planNode, substraitContext, isCudf)
  }

  def doWholeStageTransform(): WholeStageTransformContext = {
    val context = generateWholeStageTransformContext()
    if (glutenConf.getConf(GlutenConfig.CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT)) {
      wholeStageTransformerContext = Some(context)
    }
    context
  }

  /** Find all [[LeafTransformSupport]] in one WholeStageTransformer */
  private def findAllLeafTransformers(): Seq[LeafTransformSupport] = {

    def collectLeafTransformers(plan: SparkPlan): Seq[LeafTransformSupport] = plan match {

      // 找到叶子节点，则直接返回叶子结点 (BatchScanExecTransformer、FileSourceScanExecTransformer)
      case transformer: LeafTransformSupport =>
        Seq(transformer)

      // 找到HashJoinLikeExecTransformer节点，分别处理两个子分支
      case shj: HashJoinLikeExecTransformer =>
        collectLeafTransformers(shj.streamedPlan) ++ collectLeafTransformers(shj.buildPlan)

      // 普通 TransformSupport 递归处理子节点
      case t: TransformSupport =>
        t.children.flatMap(collectLeafTransformers)

      // 非 TransformSupport 节点返回空
      case _ =>
        Seq.empty
    }

    collectLeafTransformers(child)
  }

  /**
   * If containing leaf exec transformer this "whole stage" generates a RDD which itself takes care
   * of [[LeafTransformSupport]] there won't be any other RDD for leaf operator. As a result,
   * genFirstStageIterator rather than genFinalStageIterator will be invoked
   */
  private def generateWholeStageRDD(
      leafTransformers: Seq[LeafTransformSupport],
      wsCtx: WholeStageTransformContext,
      inputRDDs: ColumnarInputRDDsWrapper,
      pipelineTime: SQLMetric): RDD[ColumnarBatch] = {

    // 如果有两个叶子转换器，它们必须具有相同的分区，否则会插入exchange操作。
    // 我们应该将两个叶子转换器中相同索引的分区组合起来，并在substraitContext中一起设置它们。
    // 我们使用转置来实现这一点，你可以参考
    // 下面的图表。
    // leaf1  p11 p12 p13 p14 ... p1n
    // leaf2  p21 p22 p23 p24 ... p2n
    // 转置 =>
    // leaf1 | leaf2
    //  p11  |  p21    => substraitContext.setSplitInfo([p11, p21])
    //  p12  |  p22    => substraitContext.setSplitInfo([p12, p22])
    //  p13  |  p23    ...
    //  p14  |  p24
    //      ...
    //  p1n  |  p2n    => substraitContext.setSplitInfo([p1n, p2n])
    // 分区中的数据可能为空，例如，
    // 如果有两个使用keyGroupPartitioning的批量扫描转换器，
    // 它们具有相同的partitionValues，
    // 但对于那些不存在的分区值，某些分区可能为空
    // 详细注释说明分区转置逻辑，当有多个叶子节点时，需要将相同索引的分区组合在一起.
    val allSplitInfos = leafTransformers.map(_.getSplitInfos).transpose

    if (GlutenConfig.get.enableHdfsViewfs) {
      val viewfsToHdfsCache: mutable.Map[String, String] = mutable.Map.empty
      allSplitInfos.foreach {
        splitInfos =>
          splitInfos.foreach {
            case splitInfo: LocalFilesNode =>
              val newPaths = ViewFileSystemUtils.convertViewfsToHdfs(
                splitInfo.getPaths.asScala.toSeq,
                viewfsToHdfsCache,
                sparkContext.hadoopConfiguration)
              splitInfo.setPaths(newPaths.asJava)
          }
      }
    }

    // 通过后端API生成输入分区，这是与具体后端（Velox/ClickHouse）交互的关键点
    val inputPartitions = BackendsApiManager.getIteratorApiInstance.genPartitions(
        wsCtx,
        allSplitInfos,
        leafTransformers)

    val rdd = new GlutenWholeStageColumnarRDD(
      sparkContext,
      inputPartitions,
      inputRDDs,
      pipelineTime,
      leafInputMetricsUpdater(),
      BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
        child,
        wsCtx.substraitContext.registeredRelMap,
        wsCtx.substraitContext.registeredJoinParams,
        wsCtx.substraitContext.registeredAggregationParams
      ),
      wsCtx.enableCudf
    )

    val allInputPartitions = leafTransformers.map(_.getPartitions)
    SoftAffinity.updateFilePartitionLocations(allInputPartitions, rdd.id)

    leafTransformers.foreach {
      case batchScan: BatchScanExecTransformerBase =>
        batchScan.doPostDriverMetrics()
      case _ =>
    }

    rdd
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    // 断言检查，确保子节点支持Transform，这是WholeStageTransformer工作的前提条件
    assert(child.isInstanceOf[TransformSupport])

    val pipelineTime: SQLMetric = longMetric("pipelineTime")

    // 执行核心转换逻辑，将Spark计划转换为Substrait计划，并记录转换耗时
    val wsCtx = GlutenTimeMetric.withMillisTime {
      doWholeStageTransform()
    }(
      t => logOnLevel(GlutenConfig.get.substraitPlanLogLevel, s"$nodeName generating the substrait plan took: $t ms.")
    )

    // 包装输入RDD，为列式数据处理做准备
    //                - 如果上游是 TransformSupport 则找到 TransformSupport 上面的根节点
    //                - 否则调用 executeColumnar()
    val inputRDDs = new ColumnarInputRDDsWrapper(columnarInputRDDs)

    // 查找所有叶子节点Transformer，这些是数据源节点 (BatchScanExecTransformer、FileSourceScanExecTransformer ..)
    val leafTransformers = findAllLeafTransformers()

    if (leafTransformers.nonEmpty) {
      // 如果有叶子节点，生成包含数据扫描的完整阶段RDD
      generateWholeStageRDD(leafTransformers, wsCtx, inputRDDs, pipelineTime)
    } else {

      /**
       * 如果没有叶子节点（如ClickHouse后端的扫描或简单DataFrame），创建压缩分区RDD来处理已有的输入数据
       * the whole stage contains NO [[LeafTransformSupport]]. This is the default case for:
       *   - SCAN of clickhouse backend. See
       *     BackendsApiManager.getSettings.excludeScanExecFromCollapsedStage.
       *   - Test case where query plan is constructed from simple DataFrames, e.g.
       *     GlutenDataFrameAggregateSuite.
       *
       * In these cases, separate RDDs take care of SCAN. As a result, genFinalStageIterator rather
       * than genFirstStageIterator will be invoked.
       */
      new WholeStageZippedPartitionsRDD(
        sparkContext,
        inputRDDs,     // columnarInputRDDs
        sparkContext.getConf,
        wsCtx,
        pipelineTime,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          wsCtx.substraitContext.registeredRelMap,
          wsCtx.substraitContext.registeredJoinParams,
          wsCtx.substraitContext.registeredAggregationParams
        ),
        materializeInput
      )
    }
  }

  override def metricsUpdater(): MetricsUpdater = {
    child match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => MetricsUpdater.None
    }
  }

  private def leafInputMetricsUpdater(): InputMetricsWrapper => Unit = {
    val leaves = child.collect {
      case plan: TransformSupport if plan.children.forall(!_.isInstanceOf[TransformSupport]) =>
        plan
    }
    val leafMetricsUpdater = leaves.map(_.metricsUpdater())

    (inputMetrics: InputMetricsWrapper) => {
      leafMetricsUpdater.foreach(_.updateInputMetrics(inputMetrics))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageTransformer =
    copy(child = newChild, materializeInput = materializeInput)(transformStageId)
}

/**
 * This `columnarInputRDDs` would contain [[BroadcastBuildSideRDD]], but the dependency and
 * partition of [[BroadcastBuildSideRDD]] is meaningless. [[BroadcastBuildSideRDD]] should only be
 * used to hold the broadcast value and generate iterator for join.
 */
class ColumnarInputRDDsWrapper(columnarInputRDDs: Seq[RDD[ColumnarBatch]]) extends Serializable {
  def getDependencies: Seq[Dependency[ColumnarBatch]] = {
    assert(
      columnarInputRDDs
        .filterNot(_.isInstanceOf[BroadcastBuildSideRDD])
        .map(_.partitions.length)
        .toSet
        .size <= 1)

    columnarInputRDDs.flatMap {
      case _: BroadcastBuildSideRDD => Nil
      case rdd => new OneToOneDependency[ColumnarBatch](rdd) :: Nil
    }
  }

  def getPartitions(index: Int): Seq[Partition] = {
    columnarInputRDDs.filterNot(_.isInstanceOf[BroadcastBuildSideRDD]).map(_.partitions(index))
  }

  def getPartitionLength: Int = {
    assert(columnarInputRDDs.nonEmpty)
    val nonBroadcastRDD = columnarInputRDDs.find(!_.isInstanceOf[BroadcastBuildSideRDD])
    assert(nonBroadcastRDD.isDefined)
    nonBroadcastRDD.get.partitions.length
  }

  def getIterators(
      inputColumnarRDDPartitions: Seq[Partition],
      context: TaskContext): Seq[Iterator[ColumnarBatch]] = {
    var index = 0

    columnarInputRDDs.flatMap {
      // 如果是广播数据, 通过广播迭代方法
      case broadcast: BroadcastBuildSideRDD =>
        broadcast.genBroadcastBuildSideIterator() :: Nil

      // 笛卡尔积特殊处理
      case cartesian: CartesianColumnarBatchRDD =>
        val partition =
          inputColumnarRDDPartitions(index).asInstanceOf[CartesianColumnarBatchRDDPartition]
        index += 1
        cartesian.getIterators(partition, context)

      // 普通 RDD
      case rdd =>
        val it = rdd.iterator(inputColumnarRDDPartitions(index), context)
        index += 1
        it :: Nil
    }
  }
}

object CudfTag {
  val CudfTag = TreeNodeTag[Boolean]("org.apache.gluten.CudfTag")
}
