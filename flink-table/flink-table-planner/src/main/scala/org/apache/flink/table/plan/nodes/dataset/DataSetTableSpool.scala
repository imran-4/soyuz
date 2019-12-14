/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Spool
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.java.internal.BatchTableEnvironmentImpl
import org.apache.flink.table.catalog.{CatalogTableImpl, ObjectPath}
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
    val schema = new RowSchema(deriveRowType)
    val config = tableEnv.getConfig
    val ds = input.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)
    ds
  }

  override def copy(relTraitSet: RelTraitSet, relNode: RelNode, readType: Spool.Type, writeType: Spool.Type): Spool = {
    new DataSetTableSpool(cluster,
      traitSet,
      relNode,
      readType,
      writeType)
  }
}
