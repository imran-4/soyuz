package org.apache.flink.datalog.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datalog.executor.DatalogBatchExecutor;
import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkRelBuilder;
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
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;

import java.util.ArrayList;
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
	//	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;
	private CalciteSchema internalSchema;
//	private DatalogPlannerContext plannerContext;
	private ObjectIdentifier objectIdentifier;

	public FlinkBatchDatalogPlanner(Executor executor, TableConfig tableConfig, FunctionCatalog functionCatalog, CatalogManager catalogManager, boolean isStreamingMode) {
		this.executor = executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
		functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE);
		internalSchema = asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false));
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(functionCatalog, PlannerExpressionConverter.INSTANCE());
//		planningConfigurationBuilder =
//			new DatalogPlanningConfigurationBuilder(
//				tableConfig,
//				functionCatalog,
//				internalSchema,
//				expressionBridge);
		List<RelTraitDef> traits = new ArrayList<>();
		traits.add(ConventionTraitDef.INSTANCE);
		traits.add(FlinkRelDistributionTraitDef.INSTANCE());
		traits.add(RelCollationTraitDef.INSTANCE);
//		this.plannerContext = new DatalogPlannerContext(tableConfig, functionCatalog, this.internalSchema, traits.toArray(new RelTraitDef[0]));
//		((DatalogBatchExecutor)this.executor).getExecutionEnvironment();
	}

	private FlinkDatalogPlannerImpl createFlinkDatalogPlanner() {
		String currentCatalogName = catalogManager.getCurrentCatalog();
		String currentDatabase = catalogManager.getCurrentDatabase();
//		return plannerContext.createFlinkDatalogPlanner(currentCatalogName, currentDatabase);
		return null;
	}
//
//	private FlinkRelBuilder getFlinkRelBuilder() {
//		#
//	}

	@Override
	public List<Operation> parse(String text) {
		FlinkDatalogPlannerImpl planner = createFlinkDatalogPlanner();
		RelNode relNode = planner.parse(text);
		RelRoot relRoot = RelRoot.of(relNode, SqlKind.SELECT);
		return List.of(new PlannerQueryOperation(relRoot.project()));
	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		System.out.println("---------------******");
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
