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
import org.apache.calcite.rel.core.Spool
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalTableSpool(cluster: RelOptCluster,
                             traitSet: RelTraitSet,
                             input: RelNode,
                             readType: Spool.Type,
                             writeType: Spool.Type)
  extends Spool(cluster, traitSet, input, readType, writeType)
    with FlinkLogicalRel {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(input)
    val leftRowSize = estimateRowSize(input.getRowType)

    val ioCost = leftRowCnt + leftRowSize
    val cpuCost = leftRowCnt
    val rowCnt = leftRowCnt

    planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, readType: Spool.Type, writeType: Spool.Type): Spool = {
    new FlinkLogicalTableSpool(cluster, relTraitSet, relNode, readType, writeType)
  }
}

private class FlinkLogicalTableSpoolConverter
  extends ConverterRule(
    classOf[Spool],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableSpoolConverter") {

  override def convert(rel: RelNode): RelNode = {
    val tableSpool = rel.asInstanceOf[Spool]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val inputTable = RelOptRule.convert(tableSpool.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalTableSpool(rel.getCluster, traitSet, inputTable, tableSpool.readType, tableSpool.writeType)

  }
}


object FlinkLogicalTableSpool {

  val CONVERTER: ConverterRule = new FlinkLogicalTableSpoolConverter()

  def create(node: RelNode): FlinkLogicalTableSpool = {
    val cluster: RelOptCluster = node.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
    val inputTable = RelOptRule.convert(node.getInput(0), FlinkConventions.LOGICAL)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<")
    //    new FlinkLogicalTableSpool(cluster, traitSet, node, Spool.Type.LAZY, Spool.Type.LAZY, node.getTable)
    new FlinkLogicalTableSpool(cluster, traitSet, node, Spool.Type.LAZY, Spool.Type.LAZY)

  }
}

