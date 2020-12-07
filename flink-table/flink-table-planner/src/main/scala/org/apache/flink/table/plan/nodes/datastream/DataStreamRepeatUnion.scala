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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.RepeatUnion
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.codegen.InputFormatCodeGenerator
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.CRow

import java.util
import scala.collection.JavaConverters._

class DataStreamRepeatUnion(
                               cluster: RelOptCluster,
                               traitSet: RelTraitSet,
                               seed: RelNode,
                               iterative: RelNode,
                               all: Boolean = true,
                               iterationLimit: Int,
                               rowRelDataType: RelDataType)
    extends RepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
        with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def toString: String = {
    s"RepeatUnion(union: ($repeatUnionSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("repeatunion", repeatUnionSelectionToString)
  }

  private def repeatUnionSelectionToString: String = {
    rowRelDataType.getFieldNames.asScala.toList.mkString(", ")
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamRepeatUnion(cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      true,
      -1,
      rowRelDataType)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val config = planner.getConfig

    //    val returnType = CRowTypeInfo(schema.typeInfo)
    val generator = new InputFormatCodeGenerator(config)

    val seedDs = seed.asInstanceOf[DataStreamRel].translateToPlan(planner)

    val workingSet = seedDs
    val solutionSet = seedDs

    val iteration = solutionSet.iterate()
    //    updateCatalog(tableEnv, iteration, "__TEMP")


    val iterativeDs = iterative.asInstanceOf[DataStreamRel].translateToPlan(planner)
    val delta = iterativeDs
        .coGroup(workingSet)
    //        .where("*")
    //        .equalTo("*")
    //        .`with`(new MinusCoGroupFunction[Row](false))
    //        .withForwardedFieldsFirst("*")
    //    workingSet = delta

    //iteration.closeWith(delta, delta)

    iterativeDs

  }

}
