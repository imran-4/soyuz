package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.RepeatUnion
import org.apache.calcite.rel.logical.LogicalRepeatUnion
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._


class FlinkLogicalRepeatUnion(
                               cluster: RelOptCluster,
                               traitSet: RelTraitSet,
                               val seed: RelNode,
                               val iterative: RelNode,
                               all: Boolean,
                               iterationLimit: Int)
  extends RepeatUnion(
    cluster,
    traitSet,
    seed,
    iterative,
    all,
    iterationLimit)
    with FlinkLogicalRel {
  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = super.computeSelfCost(planner, mq)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = super.copy(traitSet, inputs)
}

  private class FlinkLogicalRepeatUnionConverter
    extends ConverterRule(
      classOf[LogicalRepeatUnion],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalRepeatUnionConverter") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val repeatUnion: LogicalRepeatUnion = call.rel(0).asInstanceOf[LogicalRepeatUnion]
      repeatUnion.all
    }

    override def convert(rel: RelNode): RelNode = {
      val repeatUnion = rel.asInstanceOf[LogicalRepeatUnion]
      val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
      val seedInputs = repeatUnion.getLeft.getInputs.asScala
        .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava

      val iterativeInputs = repeatUnion.getRight.getInputs.asScala
        .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava

      new FlinkLogicalRepeatUnion(rel.getCluster, traitSet, seedInputs.get(0), iterativeInputs.get(0), repeatUnion.all, -1)
    }
  }

  object FlinkLogicalRepeatUnion {

    val CONVERTER: ConverterRule = new FlinkLogicalRepeatUnionConverter()

    def create(seed: RelNode, iterative: RelNode, all: Boolean): FlinkLogicalRepeatUnion = {
      val cluster: RelOptCluster = seed.getCluster
      val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
      new FlinkLogicalRepeatUnion(cluster, traitSet, seed, iterative, all, -1)
    }
  }


