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
                               seed: RelNode,
                               iterative: RelNode,
                               all: Boolean = true,
                               iterationLimit: Int = -1)
  extends RepeatUnion(
    cluster,
    traitSet,
    seed,
    iterative,
    all,
    iterationLimit)
    with FlinkLogicalRel {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val children = this.getInputs.asScala
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + mq.getRowCount(child)
    }
    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    ///////////////////////
    ////// This method seems problematic...
//    new FlinkLogicalRepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
    new FlinkLogicalRepeatUnion(cluster, traitSet, inputs.get(0), inputs.get(1), true, -1)
  }
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
    val seedInput = RelOptRule.convert(repeatUnion.getSeedRel, FlinkConventions.LOGICAL)
    val iterativeInput = RelOptRule.convert(repeatUnion.getIterativeRel, FlinkConventions.LOGICAL)

    new FlinkLogicalRepeatUnion(rel.getCluster, traitSet, seedInput, iterativeInput, repeatUnion.all, repeatUnion.iterationLimit)
  }
}

object FlinkLogicalRepeatUnion {

  val CONVERTER: ConverterRule = new FlinkLogicalRepeatUnionConverter()

  def create(seed: RelNode, iterative: RelNode, all: Boolean, iterationLimit: Int = -1): FlinkLogicalRepeatUnion = {
    val cluster: RelOptCluster = seed.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
    new FlinkLogicalRepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
  }
}


