package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRepeatUnion
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRepeatUnion

class DataSetRepeatUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalRepeatUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetRepeatUnionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val repeatUnion: FlinkLogicalRepeatUnion = call.rel(0).asInstanceOf[FlinkLogicalRepeatUnion]
    true
  }

  def convert(rel: RelNode): RelNode = {
    val repeatUnion: FlinkLogicalRepeatUnion = rel.asInstanceOf[FlinkLogicalRepeatUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val seedInput = RelOptRule.convert(repeatUnion.getSeedRel, FlinkConventions.DATASET)
    val iterativeInput = RelOptRule.convert(repeatUnion.getIterativeRel, FlinkConventions.DATASET)

    new DataSetRepeatUnion(
      repeatUnion.getCluster,
      traitSet,
      seedInput,
      iterativeInput,
      true,
      -1, repeatUnion.getRowType)
  }
}

object DataSetRepeatUnionRule {
  val INSTANCE: RelOptRule = new DataSetRepeatUnionRule
}
