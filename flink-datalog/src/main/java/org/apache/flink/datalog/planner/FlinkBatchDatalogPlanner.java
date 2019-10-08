package org.apache.flink.datalog.planner;

import org.apache.calcite.jdbc.CalciteSchema;
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
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.expressions.PlannerTypeInferenceUtilImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

public class FlinkBatchDatalogPlanner implements Planner {
	private ParsableTypes programType = ParsableTypes.RULE;
	private Executor executor;
	private TableConfig tableConfig;
	private FunctionCatalog functionCatalog;
	private CatalogManager catalogManager;
	private ExpressionBridge<PlannerExpression> expressionBridge;
	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;
	private CalciteSchema internalSchema;
	private DatalogPlannerContext plannerContext;

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
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(functionCatalog, PlannerExpressionConverter.INSTANCE());
		planningConfigurationBuilder =
			new DatalogPlanningConfigurationBuilder(
				tableConfig,
				functionCatalog,
				internalSchema,
				expressionBridge);

		plannerContext = new DatalogPlannerContext(
			tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)));
	}

	public void setProgramType(ParsableTypes parsableType) {
		this.programType = parsableType;
	}


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
		// need a mechanism in this method to distinguish between query and rule
		FrameworkConfig config = plannerContext.createFrameworkConfig();
		ParserManager parserManager = new ParserManager(config, internalSchema);
		RelNode relationalTree = parserManager.parse(text, this.programType);
//
//		var planner = getFlinkPlanner();
//		SqlNode sqlNode = planner.parse(text);



		//		SqlNode parsed = planner.parse(rules);
		//		parsed match {
//			case insert: RichSqlInsert =>
//				List(SqlToOperationConverter.convert(planner, insert))
//			case query if query.getKind.belongsTo(SqlKind.QUERY) =>
//				List(SqlToOperationConverter.convert(planner, query))
//			case ddl if ddl.getKind.belongsTo(SqlKind.DDL) =>
//				List(SqlToOperationConverter.convert(planner, ddl))
//			case _ =>
//				throw new TableException(s"Unsupported query: $stmt")
//		}
		return null;
	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		if (modifyOperations.isEmpty()) {
			new ArrayList<Transformation<?>>();
		}
//		mergeParameters();
//		var relNodes = modifyOperations.map(translateToRel);
//		var optimizedRelNodes = optimize(relNodes);
//		var execNodes = translateToExecNodePlan(optimizedRelNodes);
//		translateToPlan(execNodes);
		return null;
	}

	@Override
	public String explain(List<Operation> operations, boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return new String[0];
	}
}
