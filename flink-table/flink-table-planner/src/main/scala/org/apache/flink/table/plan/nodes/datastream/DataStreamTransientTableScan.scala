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

package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.schema.TransientTable
import org.apache.flink.api.java.DataSet
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import scala.collection.JavaConverters._

class DataStreamTransientTableScan(cluster: RelOptCluster,
                                   traitSet: RelTraitSet,
                                   table: RelOptTable,
                                   tableSource: TransientTable,
                                   selectedFields: Option[Array[String]]
                               )
  extends TableScan(cluster, traitSet, table)
    with DataStreamRel {

  override def deriveRowType(): RelDataType = table.getRowType

  override def estimateRowCount(mq: RelMetadataQuery): Double = 1000L

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamTransientTableScan(cluster, traitSet, inputs.get(0).getTable, tableSource, selectedFields)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    null

  }
}
