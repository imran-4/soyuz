package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalProject(
                           cluster: RelOptCluster,
                           traits: RelTraitSet,
                           input: RelNode,
                           projects: util.List[RexNode],
                           rowType: RelDataType)
  extends Project(cluster, traits, input, projects, rowType)
    with FlinkLogicalRel {

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, list: util.List[RexNode], relDataType: RelDataType): Project = {
    new FlinkLogicalProject(cluster, traits, input, projects, rowType)
  }
}

class FlinkLogicalProjectConverter
  extends ConverterRule(
    classOf[LogicalProject],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalProjectConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[LogicalProject](0)
    true
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalProject]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)

    new FlinkLogicalProject(
      rel.getCluster,
      traitSet,
      rel,
      scan.getProjects,
      rel.getRowType
    )
  }
}

object FlinkLogicalProject {
  val CONVERTER = new FlinkLogicalProjectConverter
}
