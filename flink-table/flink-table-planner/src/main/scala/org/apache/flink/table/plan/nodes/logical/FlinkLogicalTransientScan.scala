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

package org.apache.flink.table.plan.nodes.logical


import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.schema.TransientTable
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.sources.FilterableTableSource

class FlinkLogicalTransientScan(cluster: RelOptCluster,
                                traitSet: RelTraitSet,
                                table: RelOptTable,
                                val tableSource: TransientTable,
                                val selectedFields: Option[Array[String]])
  extends TableScan(cluster, traitSet, table)
    with FlinkLogicalRel {

  def copy(traitSet: RelTraitSet, transientTable: TransientTable, selectedFields: Option[Array[String]]): FlinkLogicalTransientScan = {
    new FlinkLogicalTransientScan(cluster, traitSet, getTable, tableSource, selectedFields)
  }

  //  override def deriveRowType(): RelDataType = {
  //    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
  //    val streamingTable = table.unwrap(classOf[TableSourceTable[_]]).isStreamingMode
  //
  //    TableSourceUtil.getRelDataType(tableSource, selectedFields, streamingTable, flinkTypeFactory)
  //  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)

    val adjustedCnt: Double = tableSource match {
      case f: FilterableTableSource[_] if f.isFilterPushedDown =>
        rowCnt - 1.0
      case _ =>
        rowCnt
    }

    planner.getCostFactory.makeCost(
      adjustedCnt,
      adjustedCnt,
      adjustedCnt * estimateRowSize(getRowType))
  }
}

class FlinkLogicalTransientScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTransientScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    scan.getTable.unwrap(classOf[ListTransientTable]) != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTableScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)

    new FlinkLogicalTransientScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.getTable.unwrap(classOf[ListTransientTable]),
      Option(scan.getRowType.getFieldNames.toArray(new Array[String](0)))
    )
  }
}

object FlinkLogicalTransientScan {
  val CONVERTER = new FlinkLogicalTransientScanConverter
}
