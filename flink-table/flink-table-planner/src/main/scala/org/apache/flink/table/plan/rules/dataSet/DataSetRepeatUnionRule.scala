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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRepeatUnion
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRepeatUnion

class DataSetRepeatUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalRepeatUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetRepeatUnionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val repeatUnion: FlinkLogicalRepeatUnion = call.rel(0).asInstanceOf[FlinkLogicalRepeatUnion]
    true
  }

  def convert(rel: RelNode): RelNode = {
    val repeatUnion: FlinkLogicalRepeatUnion = rel.asInstanceOf[FlinkLogicalRepeatUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val seedInput = RelOptRule.convert(repeatUnion.getSeedRel, FlinkConventions.DATASET)
    val iterativeInput = RelOptRule.convert(repeatUnion.getIterativeRel, FlinkConventions.DATASET)

    new DataSetRepeatUnion(
      rel.getCluster,
      traitSet,
      seedInput,
      iterativeInput,
      true,
      -1, repeatUnion.getRowType)
  }
}

object DataSetRepeatUnionRule {
  val INSTANCE: RelOptRule = new DataSetRepeatUnionRule
}
