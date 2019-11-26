package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.types.Row

class DataSetTransientTableScan(cluster: RelOptCluster,
                                traitSet: RelTraitSet,
                                table: RelOptTable,
                                val tableSource: ListTransientTable //,
                                //                                val selectedFields: Option[Array[String]]
                               )
  extends TableScan(cluster, traitSet, table)
    with DataSetRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataSetTransientTableScan(cluster, traitSet, table, tableSource)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl, queryConfig: BatchQueryConfig): DataSet[Row] = {
    println(">>>>>>>>>>>>>>>>>>>>>>>++++++++++++++++++++++ INSIDE TRANSIENTTABLESCAN...")
    table.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    null
  }
}
