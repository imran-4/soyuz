package org.apache.flink.datalog.planner;

import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datalog.parser.ParsableTypes;
import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.datalog.planner.delegation.DatalogPlannerContext;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.expressions.PlannerTypeInferenceUtilImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.catalog.DatabaseCalciteSchema;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

public class FlinkBatchDatalogPlanner implements Planner {
	private Executor executor;
	private TableConfig tableConfig;
	private FunctionCatalog functionCatalog;
	private CatalogManager catalogManager;
	private ExpressionBridge<PlannerExpression> expressionBridge;
	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;
	private CalciteSchema internalSchema;
	private DatalogPlannerContext plannerContext;
	private ObjectIdentifier objectIdentifier;

//	public FlinkBatchDatalogPlanner() {
//		//change it later
////		super(null,null,null,null,false);
//	}

	public FlinkBatchDatalogPlanner(Executor executor, TableConfig tableConfig, FunctionCatalog functionCatalog, CatalogManager catalogManager, boolean isStreamingMode) {
		this.executor =  executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
		functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE);
		internalSchema = asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false));
//
//		internalSchema = asRootSchema(new DatabaseCalciteSchema(catalogManager.getCurrentDatabase(), catalogManager.getCurrentCatalog(), catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null), false));
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(functionCatalog, PlannerExpressionConverter.INSTANCE());
		planningConfigurationBuilder =
			new DatalogPlanningConfigurationBuilder(
				tableConfig,
				functionCatalog,
				internalSchema,
				expressionBridge);

//		CalciteSchema schema = asRootSchema(internalSchema.plus());
		this.plannerContext = new DatalogPlannerContext(tableConfig, functionCatalog, this.internalSchema);
	}

//	public void setProgramType(ParsableTypes parsableType) {
//		this.programType = parsableType;
//	}


	private FlinkPlannerImpl getFlinkPlanner() {
		var currentCatalogName = catalogManager.getCurrentCatalog();
		var currentDatabase = catalogManager.getCurrentDatabase();

		return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
	}

	/*
	 * By default parse() method will have parseable type "RULE". So it wont be able to parse a query if you use this method. To parse a query, you can use parseQuery() method.
	 * */
	@Override
	public List<Operation> parse(String text) {
		FrameworkConfig config = plannerContext.getFrameworkConfig();
		ParserManager parserManager = new ParserManager(config);
		RelNode relationalTree = parserManager.parse(text);

//		return convert(planner, relationalTree);
		return null;

	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		return modifyOperations
			.stream()
			.map(this::translate)
			.filter(Objects::nonNull).collect(Collectors.toList());
	}

	@Override
	public String explain(List<Operation> operations, boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		throw new UnsupportedOperationException("Please learn Datalog on your own!");
	}

	//todo:...
	private Transformation<?> translate(ModifyOperation modifyOperation) {
		return null;
	}
}
