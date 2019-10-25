package org.apache.flink.datalog.planner;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.datalog.BatchDatalogEnvironmentImpl;
import org.apache.flink.datalog.DatalogEnvironment;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.*;
import org.apache.flink.table.catalog.BasicOperatorTable;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.util.JavaScalaConversionUtil;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class DatalogPlanningConfigurationBuilder {
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
		ExpressionBridge<PlannerExpression> expressionBridge, DatalogEnvironment datalogEnvironment) {
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;

		// the converter is needed when calling temporal table functions from SQL, because
		// they reference a history table represented with a tree of table operations
		this.context = Contexts.of(expressionBridge, datalogEnvironment);
		this.planner = new VolcanoPlanner(costFactory, context);
		planner.setExecutor(new ExpressionReducer(tableConfig));
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
		this.expressionBridge = expressionBridge;
		this.rootSchema = rootSchema;
		this.datalogEnvironment = datalogEnvironment;
	}

	/**
	 * Creates a configured {@link FlinkRelBuilder} for a planning session.
	 *
	 * @param currentCatalog  the current default catalog to look for first during planning.
	 * @param currentDatabase the current default database to look for first during planning.
	 * @return configured rel builder
	 */
	public FlinkRelBuilder createRelBuilder(String currentCatalog, String currentDatabase) {
		RelOptCluster cluster = FlinkRelOptClusterFactory.create(
			planner,
			new RexBuilder(typeFactory));

		RelOptSchema relOptSchema = createCatalogReader(false, currentCatalog, currentDatabase);

		return new FlinkRelBuilder(context, cluster, relOptSchema, expressionBridge);
	}


	public FlinkDatalogPlannerImpl createFlinkPlanner(String currentCatalog, String currentDatabase) {
		return new FlinkDatalogPlannerImpl(
			createFrameworkConfig(),
			isLenient -> createCatalogReader(isLenient, currentCatalog, currentDatabase),
			planner,
			typeFactory, this.createRelBuilder(currentCatalog, currentDatabase));
	}

	/**
	 * Returns the Calcite {@link org.apache.calcite.plan.RelOptPlanner} that will be used.
	 */
	public RelOptPlanner getPlanner() {
		return planner;
	}

	/**
	 * Returns the {@link FlinkTypeFactory} that will be used.
	 */
	public FlinkTypeFactory getTypeFactory() {
		return typeFactory;
	}

	public Context getContext() {
		return context;
	}

	/**
	 * Returns the SQL parser config for this environment including a custom Calcite configuration.
	 */
	public SqlParser.Config getSqlParserConfig() {
		return JavaScalaConversionUtil.toJava(calciteConfig(tableConfig).sqlParserConfig()).orElseGet(() ->
			// we use Java lex because back ticks are easier than double quotes in programming
			// and cases are preserved
			SqlParser
				.configBuilder()
				.setParserFactory(FlinkSqlParserImpl.FACTORY)
				.setConformance(getSqlConformance())
				.setLex(Lex.JAVA)
				.build());
	}

	private FlinkSqlConformance getSqlConformance() {
		SqlDialect sqlDialect = tableConfig.getSqlDialect();
		switch (sqlDialect) {
			case HIVE:
				return FlinkSqlConformance.HIVE;
			case DEFAULT:
				return FlinkSqlConformance.DEFAULT;
			default:
				throw new TableException("Unsupported SQL dialect: " + sqlDialect);
		}
	}

	private CatalogReader createCatalogReader(
		boolean lenientCaseSensitivity,
		String currentCatalog,
		String currentDatabase) {
		SqlParser.Config sqlParserConfig = getSqlParserConfig();
		final boolean caseSensitive;
		if (lenientCaseSensitivity) {
			caseSensitive = false;
		} else {
			caseSensitive = sqlParserConfig.caseSensitive();
		}

		SqlParser.Config parserConfig = SqlParser.configBuilder(sqlParserConfig)
			.setCaseSensitive(caseSensitive)
			.build();

		return new CatalogReader(
			rootSchema,
			asList(
				asList(currentCatalog, currentDatabase),
				singletonList(currentCatalog)
			),
			typeFactory,
			CalciteConfig.connectionConfig(parserConfig));
	}

	private FrameworkConfig createFrameworkConfig() {
		return Frameworks
			.newConfigBuilder()
			.costFactory(costFactory)
			.typeSystem(typeSystem)
			.defaultSchema(this.rootSchema.plus())
			.operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
			.sqlToRelConverterConfig(
				getSqlToRelConverterConfig(
					calciteConfig(tableConfig),
					expressionBridge))
			.executor(new ExpressionReducer(tableConfig))
			.build();
		//		return Frameworks
//			.newConfigBuilder()
//			.parserConfig(getSqlParserConfig())
//			.costFactory(costFactory)
//			.typeSystem(typeSystem)
//			.operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
//			.sqlToRelConverterConfig(
//				getSqlToRelConverterConfig(
//					calciteConfig(tableConfig),
//					expressionBridge))
//			.executor(new ExpressionReducer(tableConfig))
//			.build();
	}

	private CalciteConfig calciteConfig(TableConfig tableConfig) {
		return tableConfig.getPlannerConfig()
			.unwrap(CalciteConfig.class)
			.orElseGet(CalciteConfig::DEFAULT);
	}

	/**
	 * Returns the {@link SqlToRelConverter} config.
	 */
	private SqlToRelConverter.Config getSqlToRelConverterConfig(
		CalciteConfig calciteConfig,
		ExpressionBridge<PlannerExpression> expressionBridge) {
		return JavaScalaConversionUtil.toJava(calciteConfig.sqlToRelConverterConfig()).orElseGet(
			() -> SqlToRelConverter.configBuilder()
				.withTrimUnusedFields(false)
				.withConvertTableAccess(false)
				.withInSubQueryThreshold(Integer.MAX_VALUE)
				.withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
				.build()
		);
	}

	/**
	 * Returns the operator table for this environment including a custom Calcite configuration.
	 */
	private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig, FunctionCatalog functionCatalog) {
		SqlOperatorTable baseOperatorTable = ChainedSqlOperatorTable.of(
			new BasicOperatorTable(),
			new FunctionCatalogOperatorTable(functionCatalog, typeFactory)
		);

		return JavaScalaConversionUtil.toJava(calciteConfig.sqlOperatorTable()).map(operatorTable -> {
				if (calciteConfig.replacesSqlOperatorTable()) {
					return operatorTable;
				} else {
					return ChainedSqlOperatorTable.of(baseOperatorTable, operatorTable);
				}
			}
		).orElse(baseOperatorTable);
	}
}
