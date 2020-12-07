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

package org.apache.flink.datalog.streaming;

import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.plan.logical.LogicalPlan;
import org.apache.flink.datalog.planner.DatalogPlanningConfigurationBuilder;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.rel.RelNode;

import java.util.Map;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

public class StreamingDatalogEnvironmentImpl extends StreamTableEnvironmentImpl implements StreamingDatalogEnvironment {
	private final DatalogPlanningConfigurationBuilder planningConfigurationBuilder;

	public StreamingDatalogEnvironmentImpl(
		CatalogManager catalogManager,
		ModuleManager moduleManager,
		FunctionCatalog functionCatalog,
		TableConfig config,
		StreamExecutionEnvironment executionEnvironment,
		Planner planner,
		Executor executor,
		boolean isStreaming,
		ClassLoader userClassLoader) {
		super(
			catalogManager,
			moduleManager,
			functionCatalog,
			config,
			executionEnvironment,
			planner,
			executor,
			isStreaming,
			userClassLoader);

		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(
			PlannerExpressionConverter.INSTANCE());
		this.planningConfigurationBuilder = new DatalogPlanningConfigurationBuilder(
			tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, tableConfig, isStreaming)),
			expressionBridge,
			this);
	}

	public static StreamingDatalogEnvironment create(
		StreamExecutionEnvironment executionEnvironment,
		EnvironmentSettings settings,
		TableConfig tableConfig) {

		if (!settings.isStreamingMode()) {
			throw new TableException(
				"StreamTableEnvironment can not run in batch mode for now, please use TableEnvironment.");
		}

		// temporary solution until FLINK-15635 is fixed
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		ModuleManager moduleManager = new ModuleManager();

		CatalogManager catalogManager = CatalogManager.newBuilder()
			.classLoader(classLoader)
			.config(tableConfig.getConfiguration())
			.defaultCatalog(
				settings.getBuiltInCatalogName(),
				new GenericInMemoryCatalog(
					settings.getBuiltInCatalogName(),
					settings.getBuiltInDatabaseName()))
			.executionConfig(executionEnvironment.getConfig())
			.build();

		FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = lookupExecutor(executorProperties, executionEnvironment);

		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);

		return new StreamingDatalogEnvironmentImpl(
			catalogManager,
			moduleManager,
			functionCatalog,
			tableConfig,
			executionEnvironment,
			planner,
			executor,
			settings.isStreamingMode(),
			classLoader
		);
	}

	@Override
	public Table datalogQuery(String inputProgram, String query) {
		FlinkDatalogPlannerImpl datalogPlanner = getFlinkPlanner();
		Node andOrTreeNode = datalogPlanner.parse(inputProgram, query); //node of And-Or Tree

		//todo: update catalog here, because the updated catalog will be needed in creating logical algebra (if we use scan() but may be it is not needed in transientScan()).
		LogicalPlan plan = new LogicalPlan(getRelBuilder(), super.getCatalogManager());
		plan.visit(andOrTreeNode);
		RelNode relataionalAlgebra = plan.getLogicalPlan();

		if (null != relataionalAlgebra) {
			return createTable(new PlannerQueryOperation(relataionalAlgebra));
		} else {
			throw new TableException(
				"Unsupported Datalog query!");
		}
	}

	FlinkDatalogPlannerImpl getFlinkPlanner() {
		String currentCatalogName = super.getCatalogManager().getCurrentCatalog();
		String currentDatabase = super.getCatalogManager().getCurrentDatabase();
		return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
	}

	private FlinkRelBuilder getRelBuilder() {
		String currentCatalogName = super.getCatalogManager().getCurrentCatalog();
		String currentDatabase = super.getCatalogManager().getCurrentDatabase();

		return planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase);
	}
}


