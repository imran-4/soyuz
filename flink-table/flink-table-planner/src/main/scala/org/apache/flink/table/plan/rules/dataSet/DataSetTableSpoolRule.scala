package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Spool
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetTableSpool
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalTableSpool, FlinkLogicalUnion}

class DataSetTableSpoolRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSpool],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetTableSpoolRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableSpool: FlinkLogicalTableSpool = call.rel(0).asInstanceOf[FlinkLogicalTableSpool]
    true
  }


  def convert(rel: RelNode): RelNode = {
    val tableSpool: FlinkLogicalTableSpool = rel.asInstanceOf[FlinkLogicalTableSpool]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val input = RelOptRule.convert(tableSpool, FlinkConventions.DATASET)

    new DataSetTableSpool(
      input.getCluster,
      traitSet,
      input,
      tableSpool.readType,
      tableSpool.writeType)
  }
}

object DataSetTableSpoolRule {
  val INSTANCE: RelOptRule = new DataSetTableSpoolRule
}
