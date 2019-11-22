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

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {

    val leftRowCnt = mq.getRowCount(getLeft)
    val leftRowSize = estimateRowSize(getLeft.getRowType)

    val rightRowCnt = mq.getRowCount(getRight)
    val rightRowSize = estimateRowSize(getRight.getRowType)

    val ioCost = (leftRowCnt * leftRowSize) + (rightRowCnt * rightRowSize)
    val cpuCost = leftRowCnt + rightRowCnt
    val rowCnt = leftRowCnt + rightRowCnt

    planner.getCostFactory.makeCost(1000000, 1000000, 100000)
//    val children = this.getInputs.asScala
//    val rowCnt = children.foldLeft(0D) { (rows, child) =>
//      rows + mq.getRowCount(child)
//    }
//    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
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
    val seedInput = RelOptRule.convert(repeatUnion.getLeft, FlinkConventions.LOGICAL)
    val iterativeInput = RelOptRule.convert(repeatUnion.getRight, FlinkConventions.LOGICAL)

    new FlinkLogicalRepeatUnion(rel.getCluster, traitSet, seedInput, iterativeInput, repeatUnion.all, -1)
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


