package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Spool
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetTableSpool
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalTableSpool, FlinkLogicalUnion}

class DataSetTableSpoolRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetTableSpoolRule") {

  def convert(rel: RelNode): RelNode = {
    val tableSpool: FlinkLogicalTableSpool = rel.asInstanceOf[FlinkLogicalTableSpool]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val input = RelOptRule.convert(tableSpool, FlinkConventions.LOGICAL)

    new DataSetTableSpool(
      rel.getCluster,
      traitSet,
      input, Spool.Type.LAZY, Spool.Type.LAZY, rel.getTable)
  }
}

object DataSetTableSpoolRule {
  val INSTANCE: RelOptRule = new DataSetTableSpoolRule
}
