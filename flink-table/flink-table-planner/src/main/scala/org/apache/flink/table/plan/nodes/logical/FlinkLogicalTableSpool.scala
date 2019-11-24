package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Spool, TableSpool}
import org.apache.calcite.rel.logical.LogicalTableSpool
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._

class FlinkLogicalTableSpool(cluster: RelOptCluster,
                             traitSet: RelTraitSet,
                             input: RelNode,
                             readType: Spool.Type,
                             writeType: Spool.Type,
                             table: RelOptTable) extends TableSpool(cluster, traitSet, input, readType, writeType, table) with FlinkLogicalRel {
  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(input)
    val leftRowSize = estimateRowSize(input.getRowType)

     val ioCost = leftRowCnt+leftRowSize
    val cpuCost = leftRowCnt
    val rowCnt = leftRowCnt

    planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, `type`: Spool.Type, type1: Spool.Type): Spool = {
    new FlinkLogicalTableSpool(cluster, traitSet, input, readType, writeType, table)
  }
}

private class FlinkLogicalTableSpoolConverter
  extends ConverterRule(
    classOf[TableSpool],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableSpoolConverter") {

  override def convert(rel: RelNode): RelNode = {
    val tableSpool = rel.asInstanceOf[TableSpool]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val inputTable = RelOptRule.convert(tableSpool.getInput, FlinkConventions.LOGICAL)
    new FlinkLogicalTableSpool(rel.getCluster, traitSet, inputTable, Spool.Type.LAZY, Spool.Type.LAZY, rel.getTable)
  }
}


object FlinkLogicalTableSpool {

  val CONVERTER: ConverterRule = new FlinkLogicalTableSpoolConverter()

  def create(node: RelNode): FlinkLogicalTableSpool = {
    val cluster: RelOptCluster = node.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
    new FlinkLogicalTableSpool(cluster, traitSet, node, Spool.Type.LAZY, Spool.Type.LAZY, node.getTable)
  }
}

