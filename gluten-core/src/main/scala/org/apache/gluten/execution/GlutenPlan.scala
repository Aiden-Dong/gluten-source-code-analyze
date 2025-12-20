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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.sql.execution.SparkPlan

/**
 * 由后端定义的查询计划基础接口。
 *
 * 以下 Spark API 被标记为 final，因此禁止重写：
 *   - supportsColumnar
 *   - supportsRowBased (Spark 版本 >= 3.3)
 *
 * 相反，子类需要实现以下 API：
 *   - batchType
 *   - rowType0
 *   - requiredChildConvention (可选)
 *
 * 通过提供这些 API 的实现，Gluten 查询规划器将能够找到并插入
 * 不同计划节点之间的适当转换。
 *
 * 实现 `requiredChildConvention` 是可选的，默认实现是一个约定要求序列，
 * 与输出约定完全相同。如果某些计划类型不是这种情况，则应重写该 API。
 * 例如，典型的行到列转换同时是一个查询计划节点，它需要行输入但产生列输出。
 */
trait GlutenPlan
  extends SparkPlan
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33OrLater
  with GlutenPlan.SupportsRowBasedCompatible
  with ConventionReq.KnownChildConvention {

  final override val supportsColumnar: Boolean = {
    batchType() != Convention.BatchType.None
  }

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  override def batchType(): Convention.BatchType

  override def rowType0(): Convention.RowType

  override def requiredChildConvention(): Seq[ConventionReq] = {
    // In the normal case, children's convention should follow parent node's convention.
    val childReq = Convention.of(rowType(), batchType()).asReq()
    Seq.tabulate(children.size)(
      _ => {
        childReq
      })
  }
}

object GlutenPlan {
  // To be compatible with Spark (version < 3.3)
  trait SupportsRowBasedCompatible {
    def supportsRowBased(): Boolean = {
      throw new GlutenException("Illegal state: The method is not expected to be called")
    }
  }
}
