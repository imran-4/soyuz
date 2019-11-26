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
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.{BatchTableSourceScan, DataSetTransientTableScan}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalDataSetScan, FlinkLogicalTransientScan}

class DataSetTransientTableScanRule extends ConverterRule(
  classOf[FlinkLogicalTransientScan],
  FlinkConventions.LOGICAL,
  FlinkConventions.DATASET,
  "DataSetTransientTableScanRule") {

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTransientScan = rel.asInstanceOf[FlinkLogicalTransientScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)

    new DataSetTransientTableScan(
      scan.getCluster,
      traitSet,
      scan.getTable,
      scan.tableSource,
      scan.selectedFields,
      scan.getRowType,
      scan.getTable.unwrap(classOf[ListTransientTable])
    )

    /*
   cluster: RelOptCluster,
                                traitSet: RelTraitSet,
                                table: RelOptTable,
                                tableSource: TransientTable,
                                selectedFields: Option(Array[String]),
                                rowType: RelDataType,
                                val tableSource: ListTransientTable

     */
  }
}

object DataSetTransientTableScanRule {
  val INSTANCE: RelOptRule = new DataSetTransientTableScanRule
}
