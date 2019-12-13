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

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetTableSpool
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSpool

class DataSetTableSpoolRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSpool],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetTableSpoolRule") {

  def convert(rel: RelNode): RelNode = {
    val tableSpool: FlinkLogicalTableSpool = rel.asInstanceOf[FlinkLogicalTableSpool]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val input = RelOptRule.convert(tableSpool.getInput, FlinkConventions.DATASET)

    new DataSetTableSpool(
      rel.getCluster,
      traitSet,
      input,
      tableSpool.readType,
      tableSpool.writeType)
  }
}

object DataSetTableSpoolRule {
  val INSTANCE: RelOptRule = new DataSetTableSpoolRule
}
