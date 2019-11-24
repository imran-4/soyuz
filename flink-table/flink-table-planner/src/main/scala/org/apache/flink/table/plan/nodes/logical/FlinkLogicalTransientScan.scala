package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.{FilterableTableSource, TableSource, TableSourceUtil}

class FlinkLogicalTransientScan(cluster: RelOptCluster,
                                traitSet: RelTraitSet,
                                table: RelOptTable,
                                val tableSource: TableSource[_],
                                val selectedFields: Option[Array[Int]])
  extends TableScan(cluster, traitSet, table)
    with FlinkLogicalRel {

  def copy(traitSet: RelTraitSet, tableSource: TableSource[_], selectedFields: Option[Array[Int]]): FlinkLogicalTransientScan = {
    new FlinkLogicalTransientScan(cluster, traitSet, getTable, tableSource, selectedFields)
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val streamingTable = table.unwrap(classOf[TableSourceTable[_]]).isStreamingMode

    TableSourceUtil.getRelDataType(tableSource, selectedFields, streamingTable, flinkTypeFactory)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)

    val adjustedCnt: Double = tableSource match {
      case f: FilterableTableSource[_] if f.isFilterPushedDown =>
        // ensure we prefer FilterableTableSources with pushed-down filters.
        rowCnt - 1.0
      case _ =>
        rowCnt
    }

    planner.getCostFactory.makeCost(
      adjustedCnt,
      adjustedCnt,
      adjustedCnt * estimateRowSize(getRowType))
  }


}

class FlinkLogicalTransientScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTransientScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    scan.getTable.unwrap(classOf[ListTransientTable]) != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTableScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    new FlinkLogicalTransientScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.getTable.unwrap(classOf[TableSourceTable[_]]).tableSource,
      None
    )
  }
}

object FlinkLogicalTransientScan {
  val CONVERTER = new FlinkLogicalTransientScanConverter
}
