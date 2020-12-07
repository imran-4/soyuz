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

package org.apache.flink.datalog;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.plan.logical.LogicalPlan;
import org.apache.flink.datalog.planner.DatalogPlanningConfigurationBuilder;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;

import org.apache.calcite.rel.RelNode;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.types.AbstractDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * The implementation for the Java [[BatchTableEnvironment]] that works with [[DataSet]].
 *
 * @deprecated This constructor will be removed. Use [[BatchTableEnvironment#create()]] instead.
 */
public class BatchDatalogEnvironmentImpl
	extends BatchTableEnvironmentImpl
	implements BatchDatalogEnvironment {
	//    private final OperationTreeBuilder operationTreeBuilder;
	private final FunctionCatalog functionCatalog;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();
	private CatalogManager catalogManager;
	private Executor executor;
	private TableConfig tableConfig;
	private ExecutionEnvironment executionEnvironment;
	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;

	public BatchDatalogEnvironmentImpl(
		CatalogManager catalogManager,
		TableConfig tableConfig,
		Executor executor,
		FunctionCatalog functionCatalog,
		ExecutionEnvironment executionEnvironment) {
		super(executionEnvironment, tableConfig, catalogManager, new ModuleManager());
		this.executionEnvironment = executionEnvironment;
		this.executor = executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
//        this.operationTreeBuilder = OperationTreeBuilder.create(
//                functionCatalog,
//                path -> {
//                    Optional<CatalogQueryOperation> catalogTableOperation = Optional.ofNullable(scanInternal(path).getOrElse(null));
//                    return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
//                },
//                false
//        );


		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(
			PlannerExpressionConverter.INSTANCE());
		this.planningConfigurationBuilder = new DatalogPlanningConfigurationBuilder(
			tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, tableConfig, false)),
			expressionBridge,
			this);
	}

	/**
	 * @param executionEnvironment
	 * @param settings
	 * @param tableConfig
	 *
	 * @return
	 */
	public static BatchDatalogEnvironment create(
		ExecutionEnvironment executionEnvironment,
		EnvironmentSettings settings,
		TableConfig tableConfig) {

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
			.build();

		FunctionCatalog functionCatalog = new FunctionCatalog(
			tableConfig,
			catalogManager,
			moduleManager);
		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);

		return new BatchDatalogEnvironmentImpl(
			catalogManager,
			tableConfig,
			executor,
			functionCatalog,
			executionEnvironment
		);
	}

	/**
	 * @param inputProgram
	 * @param query
	 *
	 * @return
	 */
	@Override
	public Table datalogQuery(String inputProgram, String query) {
		FlinkDatalogPlannerImpl datalogPlanner = getFlinkPlanner();
		Node andOrTreeNode = datalogPlanner.parse(inputProgram, query); //node of And-Or Tree

		//todo: update catalog here, because the updated catalog will be needed in creating logical algebra (if we use scan() but may be it is not needed in transientScan()).
		LogicalPlan plan = new LogicalPlan(this.getRelBuilder(), this.catalogManager);
		plan.visit(andOrTreeNode);
		RelNode relataionalAlgebra = plan.getLogicalPlan();

		if (null != relataionalAlgebra) {
			return createTable(new PlannerQueryOperation(relataionalAlgebra));
		} else {
			throw new TableException(
				"Unsupported Datalog query!");
		}
	}

	@Override
	public FlinkDatalogPlannerImpl getFlinkPlanner() {
		String currentCatalogName = catalogManager.getCurrentCatalog();
		String currentDatabase = catalogManager.getCurrentDatabase();
		return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
	}

	@Override
	public Table fromValues(Expression... values) {
		return null;
	}

	@Override
	public Table fromValues(AbstractDataType<?> rowType, Expression... values) {
		return null;
	}

	@Override
	public Table scan(String... tablePath) {
		return null;
	}

	@Override
	public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {

	}

	@Override
	public String explainSql(String statement, ExplainDetail... extraDetails) {
		return null;
	}

	@Override
	public String explainInternal(List<Operation> operations, ExplainDetail... extraDetails) {
		return null;
	}
}


