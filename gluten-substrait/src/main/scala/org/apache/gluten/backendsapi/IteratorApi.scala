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
package org.apache.gluten.backendsapi

import org.apache.gluten.execution.{BaseGlutenPartition, LeafTransformSupport, WholeStageTransformContext}
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

trait IteratorApi {

  /**
   * * 将 Spark 的分区信息转换为 Substrait 的 SplitInfo 为原生引擎提供文件读取的详细信息
   *
   * val splitInfo = genSplitInfo( partitionIndex = 0, partition = Seq(filePartition),
   * partitionSchema = StructType(Seq(StructField("year", IntegerType))), dataSchema =
   * StructType(Seq(StructField("id", LongType), StructField("name", StringType))), fileFormat =
   * ReadFileFormat.ParquetReadFormat, metadataColumnNames = Seq("_file_path", "_file_size"),
   * properties = Map("compression" -> "snappy") )
   */
  def genSplitInfo(
      partitionIndex: Int,
      partition: Seq[Partition],
      partitionSchema: StructType,
      dataSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo

  /**
   * * 生成 Gluten 专用的分区对象 将多个数据源的分区信息组合
   *
   * // splitInfos 的结构示例 splitInfos = Seq( Seq(table1_partition0_splitInfo,
   * table2_partition0_splitInfo), // 分区0的所有表 Seq(table1_partition1_splitInfo,
   * table2_partition1_splitInfo), // 分区1的所有表 Seq(table1_partition2_splitInfo,
   * table2_partition2_splitInfo) // 分区2的所有表 )
   */
  def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      leaves: Seq[LeafTransformSupport]): Seq[BaseGlutenPartition]

  /**
   * 为原生写入管道注入临时文件路径 必须在 genFirstStageIterator 或 genFinalStageIterator 之前调用
   *
   * // 写入 Parquet 文件前 iteratorApi.injectWriteFilesTempPath( path = "/tmp/spark-task-123/", fileName
   * \= "part-00000-uuid.parquet" )
   */
  def injectWriteFilesTempPath(path: String, fileName: String): Unit =
    throw new UnsupportedOperationException()

  /**
   * 生成**第一阶段**的迭代器（包含数据源扫描） 直接从文件系统或数据源读取数据 对应 [[GlutenWholeStageColumnarRDD]] 的使用场景
   *
   * WholeStageTransformer └── FilterExecTransformer └── BatchScanExecTransformer ← 使用
   * genFirstStageIterator
   */
  def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: InputMetricsWrapper => Unit,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq(),
      enableCudf: Boolean = false
  ): Iterator[ColumnarBatch]

  /**
   * • 生成**最终阶段**的迭代器（处理上游数据） • 从上游 RDD 接收数据进行处理 • 对应 WholeStageZippedPartitionsRDD 的使用场景
   *
   * // Join查询: SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WholeStageTransformer (Join阶段) └──
   * HashJoinExecTransformer ← 使用 genFinalStageIterator ├── InputIteratorTransformer (来自Shuffle) └──
   * InputIteratorTransformer (来自Shuffle)
   */
  // scalastyle:off argcount
  def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean = false,
      enableCudf: Boolean = false): Iterator[ColumnarBatch]
}
