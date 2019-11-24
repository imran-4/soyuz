package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.BatchTableSourceScan
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalTableSourceScan, FlinkLogicalTransientScan}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.BatchTableSource

class DataSetTransientTableScanRule extends ConverterRule(
  classOf[FlinkLogicalTransientScan],
  FlinkConventions.LOGICAL,
  FlinkConventions.DATASET,
  "DataSetTransientTableScanRule") {

  /** Rule must only match if TableScan targets a [[BatchTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]

    val sourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    sourceTable != null && !sourceTable.isStreamingMode
  }

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTransientScan = rel.asInstanceOf[FlinkLogicalTransientScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    new BatchTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.tableSource,
      scan.selectedFields
    )
  }
}

object DataSetTransientTableScanRule {
  val INSTANCE: RelOptRule = new DataSetTransientTableScanRule
}
