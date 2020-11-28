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

package org.apache.flink.datalog.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.datalog.DatalogEnvironment;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.tools.FrameworkConfig;

/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig} used to create
 * a {@link org.apache.calcite.tools.Planner} implementation for Datalog.
 */
@Internal
public class DatalogPlanningConfigurationBuilder extends PlanningConfigurationBuilder {
	private final RelOptCostFactory costFactory = new DataSetCostFactory();
	private final RelDataTypeSystem typeSystem = new FlinkTypeSystem();
	private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);
	private final RelOptPlanner planner;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final Context context;
	private final TableConfig tableConfig;
	private final FunctionCatalog functionCatalog;
	private CalciteSchema rootSchema;
	private DatalogEnvironment datalogEnvironment;

	public DatalogPlanningConfigurationBuilder(
		TableConfig tableConfig,
		FunctionCatalog functionCatalog,
		CalciteSchema rootSchema,
		ExpressionBridge<PlannerExpression> expressionBridge,
		DatalogEnvironment datalogEnvironment) {
		super(tableConfig, functionCatalog, rootSchema, expressionBridge);
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.context = super.getContext();
		this.planner = super.getPlanner();
		this.expressionBridge = expressionBridge;
		this.rootSchema = rootSchema;
		this.datalogEnvironment = datalogEnvironment;
	}

	@Override
	public FlinkDatalogPlannerImpl createFlinkPlanner(
		String currentCatalog,
		String currentDatabase) {
		return new FlinkDatalogPlannerImpl(
			super.createFrameworkConfig(),
			isLenient -> super.createCatalogReader(isLenient, currentCatalog, currentDatabase),
			this.planner,
			typeFactory);
	}
}
