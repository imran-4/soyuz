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
    //    new FlinkLogicalTableSpool(inputTable.getCluster, traitSet, inputTable, Spool.Type.LAZY, Spool.Type.LAZY)
    new FlinkLogicalTableSpool(inputTable.getCluster, traitSet, inputTable, tableSpool.readType, tableSpool.writeType)

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

