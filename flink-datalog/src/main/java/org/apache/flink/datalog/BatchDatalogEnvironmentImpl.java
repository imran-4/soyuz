package org.apache.flink.datalog;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datalog.catalog.DatalogCatalog;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.plan.logical.LogicalPlan;
import org.apache.flink.datalog.planner.DatalogPlanningConfigurationBuilder;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.BatchTableEnvImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.*;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.typeutils.FieldInfoUtils;
import scala.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

public class BatchDatalogEnvironmentImpl extends BatchTableEnvImpl implements BatchDatalogEnvironment {
	private final OperationTreeBuilder operationTreeBuilder;
	private final FunctionCatalog functionCatalog;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();
	private CatalogManager catalogManager;
	private Executor executor;
	private TableConfig tableConfig;
	//	private Planner planner;
	private ExecutionEnvironment executionEnvironment;
	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;
//	private DatalogBatchOptimizer optimizer;

	public BatchDatalogEnvironmentImpl(CatalogManager catalogManager, TableConfig tableConfig, Executor executor, FunctionCatalog functionCatalog, Planner planner, ExecutionEnvironment executionEnvironment, DatalogPlanningConfigurationBuilder planningConfigurationBuilder) {
		super(executionEnvironment, tableConfig, catalogManager, new ModuleManager());
//		super();
		this.executionEnvironment = executionEnvironment;
		this.executor = executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
//		this.planner = planner; //this should be an instance of FlinkDatalogPlanner
		this.operationTreeBuilder = OperationTreeBuilder.create(
			functionCatalog,
			path -> {
				Optional<CatalogQueryOperation> catalogTableOperation = Optional.ofNullable(scanInternal(path).getOrElse(null));
				return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
			},
			false
		);
//		this.planningConfigurationBuilder = planningConfigurationBuilder;
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(functionCatalog, PlannerExpressionConverter.INSTANCE());
		this.planningConfigurationBuilder = new DatalogPlanningConfigurationBuilder(
			tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
			expressionBridge,
			this);

//		this.optimizer = new DatalogBatchOptimizer(
//			JFunction.func(() -> tableConfig.getPlannerConfig().unwrap(CalciteConfig.class).orElse(CalciteConfig.DEFAULT())),
//			this.planningConfigurationBuilder
//		);
	}

	public static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, EnvironmentSettings settings, TableConfig tableConfig) {
		CatalogManager catalogManager = new CatalogManager(
			settings.getBuiltInCatalogName(),
			new DatalogCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));
		ModuleManager moduleManager = new ModuleManager();
		FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager, moduleManager);
		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);
		Map<String, String> plannerProperties = settings.toPlannerProperties();
//		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
//			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);   //this should create FlinkDatalogPlanner

//		DatalogPlanningConfigurationBuilder planningConfigurationBuilder =
//			new DatalogPlanningConfigurationBuilder(
//				tableConfig,
//				functionCatalog,
//				asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
//				expressionBridge);

		return new BatchDatalogEnvironmentImpl(
			catalogManager,
			tableConfig,
			executor,
			functionCatalog,
			null,
			executionEnvironment,
			null//			planningConfigurationBuilder
		);
	}

	@Override
	public Table datalogQuery(String inputProgram, String query) {
		FlinkDatalogPlannerImpl datalogPlanner = getFlinkPlanner();
		Node andOrTreeNode = datalogPlanner.parse(inputProgram, query); //node of And-Or Tree

		//todo: update catalog here, because the updated catalog will be needed in creating logical algebra (if we use scan() but may be it is not needed in transientScan()).
		LogicalPlan plan = new LogicalPlan(this.getRelBuilder(), this.catalogManager);
		plan.visit(andOrTreeNode);
		assert andOrTreeNode != null;
		RelNode relataionalAlgebra = plan.getLogicalPlan();
		System.out.println(relataionalAlgebra);
//		DataSetRel dataSetRel = null;

		if (null != relataionalAlgebra) {
			return createTable(new PlannerQueryOperation(relataionalAlgebra));
		} else {
			throw new TableException(
				"Unsupported Datalog query!");
		}
	}

	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
		throw new UnsupportedOperationException("Not supported."); //there are no functions in datalog
	}

	@Override
	public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
		throw new UnsupportedOperationException("Not supported"); //there are no functions in datalog
	}

	@Override
	public <T> Table fromDataSet(DataSet<T> dataSet) {
		return this.createTable(asQueryOperation(dataSet, Optional.ofNullable(null)));
	}

	@Override
	public <T> Table fromDataSet(DataSet<T> dataSet, String fields) {
		List<Expression> exprs = ExpressionParser
			.parseExpressionList(fields);

		return createTable(asQueryOperation(dataSet, Optional.of(exprs)));
	}

	@Override
	public <T> void registerDataSet(String name, DataSet<T> dataSet) {
		registerTable(name, fromDataSet(dataSet));
	}

	@Override
	public <T> void registerDataSet(String name, DataSet<T> dataSet, String fields) {
		registerTable(name, fromDataSet(dataSet, fields));
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, Class<T> clazz) {
		return translate(table, TypeExtractor.createTypeInfo(clazz));
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo) {
		return translate(table, typeInfo);//(typeInfo);
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, Class<T> clazz, BatchQueryConfig queryConfig) {
		return translate(table, TypeExtractor.createTypeInfo(clazz));
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo, BatchQueryConfig queryConfig) {
		return translate(table, typeInfo);
	}

	@Override
	public void sqlUpdate(String stmt, BatchQueryConfig config) {
		throw new UnsupportedOperationException("");
	}

	@Override
	public void insertInto(Table table, BatchQueryConfig queryConfig, String sinkPath, String... sinkPathContinued) {
		throw new UnsupportedOperationException("Not implemented yet. Will implement later.");
	}

	@Override
	public Table fromTableSource(TableSource<?> source) {
		return createTable(new TableSourceQueryOperation<>(source, true));
	}

	@Override
	public void registerCatalog(String catalogName, Catalog catalog) {
		catalogManager.registerCatalog(catalogName, catalog);
	}

	@Override
	public Optional<Catalog> getCatalog(String catalogName) {
		return catalogManager.getCatalog(catalogName);
	}

	@Override
	public void loadModule(String moduleName, Module module) {

	}

	@Override
	public void unloadModule(String moduleName) {

	}

	@Override
	public void registerFunction(String name, ScalarFunction function) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void registerTable(String name, Table table) {
		if (((TableImpl) table).getTableEnvironment() != this) {
			throw new TableException(
				"Only tables that belong to this TableEnvironment can be registered.");
		}
		CatalogBaseTable view = new QueryOperationCatalogView(table.getQueryOperation());
		catalogManager.createTable(view, catalogManager.qualifyIdentifier(
			catalogManager.getBuiltInCatalogName(),
			catalogManager.getBuiltInDatabaseName(),
			name
		), false);
	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {
		// the implementation of this function is similar to the one in TableEnvImpl, since its purpose is same.
		TableSourceValidation.validateTableSource(tableSource);

		if (!(tableSource instanceof BatchTableSource<?> || tableSource instanceof InputFormatTableSource<?>)) {
			throw new TableException("Only BatchTableSource or InputFormatTableSource are allowed here.");
		}
		//---------------------------------------------------------------------
		Optional<CatalogBaseTable> table = catalogManager.getTable(catalogManager.qualifyIdentifier(name));
		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSource().isPresent()) {
					throw new ValidationException(String.format("Table '%s' already exists. Please choose a different name.", name));
				} else {
					ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable.sourceAndSink(
						tableSource,
						sourceSinkTable.getTableSink().get(),
						true);
					catalogManager.alterTable(sourceAndSink, catalogManager.qualifyIdentifier(
						catalogManager.getBuiltInCatalogName(),
						catalogManager.getBuiltInDatabaseName(),
						name), false);
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			ConnectorCatalogTable source = ConnectorCatalogTable.source(tableSource, true);
			catalogManager.createTable(source, catalogManager.qualifyIdentifier(
				catalogManager.getBuiltInCatalogName(),
				catalogManager.getBuiltInDatabaseName(),
				name), false);
		}
	}

	@Override
	public void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {
		registerTableSink(name, tableSink.configure(fieldNames, fieldTypes));
	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {
		if (configuredSink.getTableSchema().getFieldCount() == 0) {
			throw new TableException("Table schema cannot be empty.");
		}
		registerTableSinkInternal(name, configuredSink);
	}

	@Override
	public Table scan(String... tablePath) {
		return Optional.ofNullable(scanInternal(tablePath)).map(x -> createTable(x.get()))
			.orElseThrow(() -> new ValidationException(String.format(
				"Table '%s' was not found.",
				String.join(".", tablePath))));
	}

	@Override
	public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {
		throw new UnsupportedOperationException("Not supported yet. It may be implemented later.");
	}

	@Override
	public BatchTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return null;
	}

	@Override
	public String[] listCatalogs() {
		return this.catalogManager.getCatalogs().toArray(new String[0]);
	}

	@Override
	public String[] listModules() {
		return new String[0];
	}

	@Override
	public String[] listDatabases() {
		List<String> databases = new ArrayList<>();
		for (String c : this.catalogManager.getCatalogs()) {
			boolean isCatalogPresent = this.catalogManager.getCatalog(c).isPresent();
			if (isCatalogPresent)
				databases.addAll(this.catalogManager.getCatalog(c).get().listDatabases());
		}
		return databases.toArray(new String[0]);
	}

	@Override
	public String[] listTables() {
		List<String> tables = new ArrayList<>();
		for (String catalog : this.catalogManager.getCatalogs()) {
			boolean isCatalogPresent = this.catalogManager.getCatalog(catalog).isPresent();
			if (isCatalogPresent) {
				for (String database : this.catalogManager.getCatalog(catalog).get().listDatabases()) {
					try {
						tables.addAll(this.catalogManager.getCatalog(catalog).get().listTables(database));
					} catch (DatabaseNotExistException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return tables.toArray(new String[0]);
	}

	@Override
	public String[] listUserDefinedFunctions() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String[] listFunctions() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String explain(Table table) {
		return explain(table, false);
	}

//	@Override
//	public String explain(Table table, boolean extended) {
//		return planner.explain(Collections.singletonList(table.getQueryOperation()), extended);
//	}
//
//	@Override
//	public String explain(boolean extended) {
//		List<Operation> operations = bufferedModifyOperations.stream()
//			.map(o -> (Operation) o).collect(Collectors.toList());
//		return planner.explain(operations, extended);
//	}
//
//	@Override
//	public String[] getCompletionHints(String statement, int position) {
//		return planner.getCompletionHints(statement, position);
//	}

	@Override
	public Table sqlQuery(String query) {
		throw new UnsupportedOperationException("This implementation is only for Datalog queries, not SQL queries. Use Table API for SQL queries.");
	}

	@Override
	public void sqlUpdate(String stmt) {
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public String getCurrentCatalog() {
		return this.catalogManager.getCurrentCatalog();
	}

	@Override
	public void useCatalog(String catalogName) {
		this.catalogManager.setCurrentCatalog(catalogName);
	}

	@Override
	public String getCurrentDatabase() {
		return this.catalogManager.getCurrentDatabase();
	}

	@Override
	public void useDatabase(String databaseName) {
		this.catalogManager.setCurrentDatabase(databaseName);
	}

	@Override
	public TableConfig getConfig() {
		return this.tableConfig;
	}

	@Override
	public JobExecutionResult execute(String jobName) {
		//translate(bufferedModifyOperations);
		bufferedModifyOperations.clear();
		try {
			return this.executor.execute(jobName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private ObjectIdentifier getTemporaryObjectIdentifier(String name) {
		return catalogManager.qualifyIdentifier(
			catalogManager.getBuiltInCatalogName(),
			catalogManager.getBuiltInDatabaseName(),
			name
		);
	}

	private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
		// for now, similar to the one of TableEnvironmentImpl
		Optional<CatalogBaseTable> table = getCatalogTable(
			catalogManager.getBuiltInCatalogName(),
			catalogManager.getBuiltInDatabaseName(),
			name);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSink().isPresent()) {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable
						.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, true);
					catalogManager.alterTable(sourceAndSink, getTemporaryObjectIdentifier(name), false);
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, true);
			catalogManager.createTable(sink, getTemporaryObjectIdentifier(name), false);
		}
	}

	private Optional<CatalogBaseTable> getCatalogTable(String... name) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(name);
		return catalogManager.getTable(objectIdentifier);
	}

	public TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}

	public void validateTableSource(TableSource<?> tableSource) {
		TableSourceValidation.validateTableSource(tableSource);
	}

//	private void translate(List<ModifyOperation> modifyOperations) {
//		List<Transformation<?>> transformations = planner.translate(modifyOperations);
//		executor.apply(transformations);
//	}
//
//	private void buffer(List<ModifyOperation> modifyOperations) {
//		bufferedModifyOperations.addAll(modifyOperations);
//	}

	@Override
	public Option<CatalogQueryOperation> scanInternal(String... tablePath) {
		return super.scanInternal(tablePath);
//		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(tablePath);
//		return catalogManager.getTable(objectIdentifier)
//			.map((t) -> new CatalogQueryOperation(objectIdentifier, t.getSchema()));
	}

	private <T> DataSetQueryOperation<T> asQueryOperation(DataSet<T> dataSet, Optional<List<Expression>> fields) {
		TypeInformation<T> dataSetType = dataSet.getType();

		FieldInfoUtils.TypeInfoSchema typeInfoSchema = fields.map(f -> FieldInfoUtils.getFieldsInfo(
			dataSetType,
			f.toArray(new Expression[0]))).orElseGet(() -> FieldInfoUtils.getFieldsInfo(dataSetType));
		return new DataSetQueryOperation<T>(
			dataSet,
			typeInfoSchema.getIndices(),
			typeInfoSchema.toTableSchema());
	}

	public FlinkDatalogPlannerImpl getFlinkPlanner() {
		String currentCatalogName = catalogManager.getCurrentCatalog();
		String currentDatabase = catalogManager.getCurrentDatabase();
		return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
	}
//
//	public FlinkDatalogPlannerImpl getFlinkPlanner() {
//		String currentCatalogName = catalogManager.getCurrentCatalog();
//		String currentDatabase = catalogManager.getCurrentDatabase();
//		return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
//	}

//	public <T> DataSet<T> translate(Table table, TypeInformation<T> tpe) {
//		QueryOperation queryOperation = table.getQueryOperation();
//		RelNode relNode = getRelBuilder().tableOperation(queryOperation).build();
//		var dataSetPlan = optimizer.optimize(relNode);
//		return translate(dataSetPlan, getTableSchema(queryOperation.getTableSchema().getFieldNames(), dataSetPlan), tpe);
//	}
//
//	public <T> DataSet<T> translate(RelNode logicalPlan, TableSchema logicalType, TypeInformation<T> tpe) {
//		validateInputTypeInfo(tpe);
//		if (logicalPlan instanceof DataSetRel) {
//			var node = (DataSetRel) logicalPlan;
//			var plan = node.translateToPlan(this, new BatchQueryConfig());
//			var conversion =
//				getConversionMapper(
//					plan.getType(),
//					logicalType,
//					tpe,
//					"DataSetSinkConversion");
//			if (conversion.isEmpty()) {
//				DataSet<T> dataSet = (DataSet<T>) plan;
//				return dataSet;
//			}
//			Optional<Optional<MapFunction<Row, T>>> mapFunction = Optional.of(conversion);
//			return plan.map(mapFunction.get().orElseThrow())
//				.returns(tpe)
//				.name("to: ${tpe.getTypeClass.getSimpleName}");
//		} else {
//			throw new TableException("Cannot generate DataSet due to an invalid logical plan. " +
//				"This is a bug and should not happen. Please file an issue.");
//		}
//	}
//
//	private TableSchema getTableSchema(String[] originalNames, RelNode optimizedPlan) {
//		DataType[] fieldTypes = optimizedPlan.getRowType().getFieldList().stream().map(RelDataTypeField::getType)
//			.map(FlinkTypeFactory::toTypeInfo)
//			.map(TypeConversions::fromLegacyInfoToDataType)
//			.collect(Collectors.toList()).toArray(new DataType[0]);
//		return TableSchema.builder().fields(originalNames, fieldTypes).build();
//	}
}


