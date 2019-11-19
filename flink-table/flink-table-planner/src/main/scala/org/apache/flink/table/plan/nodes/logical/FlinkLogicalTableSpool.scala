package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Spool, TableSpool}
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._

class FlinkLogicalTableSpool(cluster: RelOptCluster,
                             traitSet: RelTraitSet,
                             input: RelNode,
                             readType: Spool.Type,
                             writeType: Spool.Type,
                             table: RelOptTable) extends TableSpool(cluster, traitSet, input, readType, writeType, table) with FlinkLogicalRel {

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, `type`: Spool.Type, type1: Spool.Type): Spool = {
    new FlinkLogicalTableSpool(cluster, traitSet, input, readType, writeType, table)
  }
}

private class FlinkLogicalTableSpoolConverter
  extends ConverterRule(
    classOf[FlinkLogicalTableSpool],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableSpoolConverter") {

  override def convert(rel: RelNode): RelNode = {
    val tableSpool = rel.asInstanceOf[FlinkLogicalTableSpool]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val seedInputs = tableSpool.getInputs.asScala
      .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava


    new FlinkLogicalTableSpool(rel.getCluster, traitSet, seedInputs.get(0), Spool.Type.LAZY, Spool.Type.LAZY, rel.getTable)
  }
}


object FlinkLogicalTableSpoolScan {

  val CONVERTER: ConverterRule = new FlinkLogicalTableSpoolConverter()

  def create(node: RelNode): FlinkLogicalTableSpool = {
    val cluster: RelOptCluster = node.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
    new FlinkLogicalTableSpool(cluster, traitSet, node, Spool.Type.LAZY, Spool.Type.LAZY, node.getTable)
  }
}

