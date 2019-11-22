package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.types.Row

class DataSetProject(cluster: RelOptCluster,
                     traits: RelTraitSet,
                     input: RelNode) extends SingleRel(cluster, traits, input) with DataSetRel {

  override def deriveRowType(): RelDataType = rowType

  override def estimateRowCount(mq: RelMetadataQuery): Double = 1000L

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetProject(
      cluster,
      traitSet,
      null
    )
  }

  def translateToPlan(tableEnv: BatchTableEnvImpl, queryConfig: BatchQueryConfig): DataSet[Row] = {
    null
  }
}


