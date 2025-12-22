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
import org.apache.gluten.metrics.{GlutenTimeMetric, IMetrics}

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

private[gluten] class ZippedPartitionsPartition(
    override val index: Int,
    val inputColumnarRDDPartitions: Seq[Partition])
  extends Partition {}

/**
 * * 是 Gluten 中用于处理**中间阶段**（非叶子节点阶段）的 RDD，它将多个上游 RDD 的分区"拉链式"组合，然后交给原生引擎执行。
 * @param sc
 * @param rdds
 * @param sparkConf
 * @param resCtx
 * @param pipelineTime
 * @param updateNativeMetrics
 * @param materializeInput
 */
class WholeStageZippedPartitionsRDD(
    @transient private val sc: SparkContext,
    var rdds: ColumnarInputRDDsWrapper, // 上游RDD包装器
    sparkConf: SparkConf,
    resCtx: WholeStageTransformContext, //  核心处理逻辑上下文
    pipelineTime: SQLMetric, //  性能指标
    updateNativeMetrics: IMetrics => Unit, //  指标更新函数
    materializeInput: Boolean) // 是否物化输入
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        // 将通用的 Partition 转换为 ZippedPartitionsPartition
        val partitions = split.asInstanceOf[ZippedPartitionsPartition].inputColumnarRDDPartitions

        val inputIterators: Seq[Iterator[ColumnarBatch]] = rdds.getIterators(partitions, context)

        BackendsApiManager.getIteratorApiInstance
          .genFinalStageIterator(
            context,
            inputIterators,
            sparkConf,
            resCtx.root,
            pipelineTime,
            updateNativeMetrics,
            split.index,
            materializeInput,
            resCtx.enableCudf
          )
    }
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](rdds.getPartitionLength) {
      i => new ZippedPartitionsPartition(i, rdds.getPartitions(i))
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
