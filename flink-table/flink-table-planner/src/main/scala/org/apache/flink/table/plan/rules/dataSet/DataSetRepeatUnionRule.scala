package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRepeatUnion
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalRepeatUnion, FlinkLogicalUnion}

class DataSetRepeatUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetRepeatUnionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val repeatUnion: FlinkLogicalRepeatUnion = call.rel(0).asInstanceOf[FlinkLogicalRepeatUnion]
    repeatUnion.all
  }

  def convert(rel: RelNode): RelNode = {
    val repeatUnion: FlinkLogicalRepeatUnion = rel.asInstanceOf[FlinkLogicalRepeatUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)

    val seedInput = RelOptRule.convert(repeatUnion.getLeft, FlinkConventions.LOGICAL)

    val iterativeInputs = RelOptRule.convert(repeatUnion.getRight, FlinkConventions.LOGICAL)

    new DataSetRepeatUnion(
      rel.getCluster,
      traitSet,
      seedInput,
      iterativeInputs,
      true,
      -1, rel.getRowType)
  }
}

object DataSetRepeatUnionRule {
  val INSTANCE: RelOptRule = new DataSetRepeatUnionRule
}
