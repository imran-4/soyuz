package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.RepeatUnion
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.runtime.{CountPartitionFunction, LimitFilterFunction}
import org.apache.flink.types.Row

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

  def repeatUnionSelectionToString: String = ???

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("repeatunion", repeatUnionSelectionToString)
  }

  override def translateToPlan(
                                tableEnv: BatchTableEnvImpl,
                                queryConfig: BatchQueryConfig): DataSet[Row] = {

    val config = tableEnv.getConfig

    val seedDs = seed.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)
    val iterativeDs = iterative.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    val currentParallelism = seedDs.getExecutionEnvironment.getParallelism

    /*
    Implement Iteration somewhere here...
    IterationState workset = getInitialState();
    IterationState solution = getInitialSolution();

    while (!terminationCriterion()) {
      (delta, workset) = step(workset, solution);

      solution.update(delta)
    }
    setFinalState(solution);
     */

    //todo:
    null
  }

}
