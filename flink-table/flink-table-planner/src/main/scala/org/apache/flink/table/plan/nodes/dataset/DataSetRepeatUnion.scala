package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.RepeatUnion
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class DataSetRepeatUnion(
                          cluster: RelOptCluster,
                          traitSet: RelTraitSet,
                          seed: RelNode,
                          iterative: RelNode,
                          all: Boolean = true,
                          iterationLimit: Int,
                          rowRelDataType: RelDataType)
  extends RepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
    with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def toString: String = {
    s"RepeatUnion(union: ($repeatUnionSelectionToString))"
  }

  private def repeatUnionSelectionToString: String = {
    rowRelDataType.getFieldNames.asScala.toList.mkString(", ")
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("repeatunion", repeatUnionSelectionToString)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataSetRepeatUnion(cluster, traitSet, seed, iterative, true, -1, rowRelDataType)
  }

  override def translateToPlan(
                                tableEnv: BatchTableEnvImpl,
                                queryConfig: BatchQueryConfig): DataSet[Row] = {

    //todo: semi-naive evaluation algorithm would be implemented here....

    // the following is just a sketch of the algorithm and not properly implemented and tested yet....
    val config = tableEnv.getConfig
    val seedDs = seed.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)
    var all = seedDs
    val iterativeDs = iterative.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    val iterativeDsInitial = iterativeDs.iterate(1000)

    val delta = iterativeDsInitial.filter(new FilterFunction[Row] {
      override def filter(value: Row): Boolean = {
        true
      }
    }).distinct()

    all = all.union(delta)

    val finalAll = iterativeDsInitial.closeWith(all)

    finalAll
  }

}
