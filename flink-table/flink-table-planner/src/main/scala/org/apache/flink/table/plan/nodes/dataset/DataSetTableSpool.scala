package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Spool, TableSpool}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.types.Row

class DataSetTableSpool(cluster: RelOptCluster,
                        traitSet: RelTraitSet,
                        input: RelNode,
                        readType: Spool.Type,
                        writeType: Spool.Type,
                        table: RelOptTable) extends TableSpool(cluster, traitSet, input, readType, writeType, table) with DataSetRel {

  override def translateToPlan(tableEnv: BatchTableEnvImpl, queryConfig: BatchQueryConfig): DataSet[Row] = {
    null
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, readType: Spool.Type, writeType: Spool.Type): Spool = {
    new DataSetTableSpool(cluster,
      traitSet,
      input,
      readType,
      writeType,
      table)
  }
}
