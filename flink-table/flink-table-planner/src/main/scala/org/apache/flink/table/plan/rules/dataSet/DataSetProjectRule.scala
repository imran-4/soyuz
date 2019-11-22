package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalDataSetScan, FlinkLogicalProject}

class DataSetProjectRule extends ConverterRule(
  classOf[FlinkLogicalDataSetScan],
  FlinkConventions.LOGICAL,
  FlinkConventions.DATASET,
  "DataSetProjectRule") {

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalProject = rel.asInstanceOf[FlinkLogicalProject]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)

    null
  }
}

object DataSetProjectRule {
  val INSTANCE = new DataSetProjectRule
}
