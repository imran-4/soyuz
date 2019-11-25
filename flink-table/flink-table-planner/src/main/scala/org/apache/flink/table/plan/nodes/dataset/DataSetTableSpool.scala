package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Spool, TableSpool}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row

class DataSetTableSpool(cluster: RelOptCluster,
                        traitSet: RelTraitSet,
                        input: RelNode,
                        readType: Spool.Type,
                        writeType: Spool.Type)
  extends Spool(cluster, traitSet, input, readType, writeType)
  with DataSetRel {

  override def deriveRowType(): RelDataType = input.getRowType

  override def estimateRowCount(mq: RelMetadataQuery): Double = 1000L

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl, queryConfig: BatchQueryConfig): DataSet[Row] = {
    val config = tableEnv.getConfig
    input.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)
//    convertToInternalRow(schema, inputDataSet.asInstanceOf[DataSet[Any]], fieldIdxs, config, None)
    null
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, readType: Spool.Type, writeType: Spool.Type): Spool = {
    new DataSetTableSpool(cluster,
      traitSet,
      relNode,
      readType,
      writeType)
  }
}
