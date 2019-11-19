package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Spool, TableSpool}
import org.apache.calcite.rel.logical.LogicalRepeatUnion
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._

class FlinkLogicalTableSpoolScan(cluster: RelOptCluster,
                                 traitSet: RelTraitSet,
                                 input : RelNode,
                                 readType : Spool.Type,
                                 writeType : Spool.Type,
                                 table: RelOptTable) extends TableSpool(cluster, traitSet, input, readType, writeType, table) with FlinkLogicalRel {

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, `type`: Spool.Type, type1: Spool.Type): Spool = {
    new FlinkLogicalTableSpoolScan(cluster, traitSet, input, readType, writeType, table)
  }
}

  private class FlinkLogicalTableSpoolConverter
    extends ConverterRule(
      classOf[LogicalRepeatUnion],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalTableSpoolConverter") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val repeatUnion: LogicalRepeatUnion = call.rel(0).asInstanceOf[LogicalRepeatUnion]
      repeatUnion.all
    }

    override def convert(rel: RelNode): RelNode = {
      val repeatUnion = rel.asInstanceOf[LogicalRepeatUnion]
      val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
      val seedInputs = repeatUnion.getInputs.asScala
        .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava


      new FlinkLogicalTableSpoolScan(rel.getCluster, traitSet, seedInputs.get(0), Spool.Type.LAZY, Spool.Type.LAZY, rel.getTable)
    }
  }


object FlinkLogicalTableSpoolScan {

    val CONVERTER: ConverterRule = new FlinkLogicalTableSpoolConverter()

    def create(seed: RelNode, iterative: RelNode, all: Boolean): FlinkLogicalTableSpoolScan = {
      val cluster: RelOptCluster = seed.getCluster
      val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
      new FlinkLogicalTableSpoolScan(cluster, traitSet, seed, Spool.Type.LAZY, Spool.Type.LAZY, null)
    }
  }

