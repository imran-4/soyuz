/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.RepeatUnion
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.java.internal.BatchTableEnvironmentImpl
import org.apache.flink.table.runtime.MinusCoGroupFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class DataSetRepeatUnion(
                          cluster: RelOptCluster,
                          traitSet: RelTraitSet,
                          seed: RelNode,
                          iterative: RelNode,
                          all: Boolean = true,
                          iterationLimit: Int,
                          rowRelDataType: RelDataType)
  extends RepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
    with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def toString: String = {
    s"RepeatUnion(union: ($repeatUnionSelectionToString))"
  }

  private def repeatUnionSelectionToString: String = {
    rowRelDataType.getFieldNames.asScala.toList.mkString(", ")
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("repeatunion", repeatUnionSelectionToString)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataSetRepeatUnion(cluster, traitSet, inputs.get(0), inputs.get(1), true, -1, rowRelDataType)
  }

  override def translateToPlan(
                                tableEnv: BatchTableEnvImpl,
                                queryConfig: BatchQueryConfig): DataSet[Row] = {
    val config = tableEnv.getConfig
    val seedDs = seed.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig).distinct().asInstanceOf[DataSet[Row]]

    tableEnv match {
      case btei: BatchTableEnvironmentImpl =>
        btei.registerTable("tc", btei.fromDataSet(seedDs))
      case _ =>
    }
    val iterativeDs = iterative.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig).distinct()

    val workingSet: DataSet[Row] = iterativeDs
    val solutionSet: DataSet[Row] = seedDs
    val maxIterations: Int = Int.MaxValue
    val iteration = solutionSet.iterateDelta(workingSet, 100, (0 until seedDs.getType.getTotalFields):_*)
    val deltas = iteration.getWorkset
      .coGroup(iteration.getSolutionSet) //no subtract operator for dataset so now using coGroup...
      .where((0 until seedDs.getType.getTotalFields):_*)
      .equalTo((0 until seedDs.getType.getTotalFields):_*)
      .`with`(new MinusCoGroupFunction[Row](true))
      .distinct()
    val newSolutionSet = iteration.getWorkset.union(deltas)
    val result = iteration.closeWith(newSolutionSet, deltas)
    result
  }
}
