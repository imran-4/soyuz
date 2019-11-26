package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetTableSpool
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSpool

class DataSetTableSpoolRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSpool],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetTableSpoolRule") {

  def convert(rel: RelNode): RelNode = {
    val tableSpool: FlinkLogicalTableSpool = rel.asInstanceOf[FlinkLogicalTableSpool]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val input = RelOptRule.convert(tableSpool.getInput, FlinkConventions.DATASET)

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
