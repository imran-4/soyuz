package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetTransientTableScan
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTransientScan

class DataSetTransientTableScanRule extends ConverterRule(
  classOf[FlinkLogicalTransientScan],
  FlinkConventions.LOGICAL,
  FlinkConventions.DATASET,
  "DataSetTransientTableScanRule") {

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTransientScan = rel.asInstanceOf[FlinkLogicalTransientScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)

    new DataSetTransientTableScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.getTable.unwrap(classOf[ListTransientTable]) //,
      //      input.selectedFields
    )
  }
}

object DataSetTransientTableScanRule {
  val INSTANCE: RelOptRule = new DataSetTransientTableScanRule
}
