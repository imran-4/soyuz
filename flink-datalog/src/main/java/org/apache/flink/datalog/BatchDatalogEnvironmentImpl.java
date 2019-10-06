package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datalog.catalog.DatalogCatalog;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.operations.*;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.typeutils.FieldInfoUtils;

import java.util.*;
import java.util.stream.Collectors;

public class BatchDatalogEnvironmentImpl implements BatchDatalogEnvironment {
	private final OperationTreeBuilder operationTreeBuilder;
	private final FunctionCatalog functionCatalog;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();
	private CatalogManager catalogManager;
	private Executor executor;
	private TableConfig tableConfig;
	private Planner planner;
	private ExecutionEnvironment executionEnvironment;

	public BatchDatalogEnvironmentImpl(CatalogManager catalogManager, TableConfig tableConfig, Executor executor, FunctionCatalog functionCatalog, Planner planner, ExecutionEnvironment executionEnvironment) {
		this.executionEnvironment = executionEnvironment;
		this.executor = executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
		this.planner = planner; //this should be an instance of FlinkDatalogPlanner
		this.operationTreeBuilder = OperationTreeBuilder.create(
			functionCatalog,
			path -> {
				Optional<CatalogQueryOperation> catalogTableOperation = scanInternal(path);
				return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
			},
			false
		);
	}

	public static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, EnvironmentSettings settings, TableConfig tableConfig) {
		CatalogManager catalogManager = new CatalogManager(
			settings.getBuiltInCatalogName(),
			new DatalogCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));
		FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);
		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);   //this should create FlinkDatalogPlanner
		return new BatchDatalogEnvironmentImpl(
			catalogManager,
			tableConfig,
			executor,
			functionCatalog,
			planner,
			executionEnvironment
		);
	}

	@Override
	public void evaluateDatalogRules(String program) {
		List<Operation> operations = planner.parse(program);

	}

	@Override
	public <T> DataSet<T> datalogQuery(String query) {
		List<Operation> operations = planner.parse(query); //in this method, either do implementation using visitor or listener

		if (operations.size() != 1) {
			throw new ValidationException(
				"Unsupported Datalog query! datalogQuery() only accepts a single Datalog query.");
		}
		Operation operation = operations.get(0);
		System.out.println(operation.asSummaryString());
		//		return createTable((QueryOperation) operation);
		return null;
	}

	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public <T> Table fromDataSet(DataSet<T> dataSet) {
		return createTable(asQueryOperation(dataSet, null));
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
		throw new UnsupportedOperationException("Not implemented yet. But to be implemented soon.");
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo) {
		throw new UnsupportedOperationException("Not implemented yet. But to be implemented soon.");
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, Class<T> clazz, BatchQueryConfig queryConfig) {
		throw new UnsupportedOperationException("Not implemented yet. But to be implemented soon.");
	}

	@Override
	public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo, BatchQueryConfig queryConfig) {
		throw new UnsupportedOperationException("Not implemented yet. But to be implemented soon.");
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
	public void registerFunction(String name, ScalarFunction function) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void registerTable(String name, Table table) {
		if (((TableImpl) table).getTableEnvironment() != this) {
			throw new TableException(
				"Only tables that belong to this TableEnvironment can be registered.");
		}
		QueryOperationCatalogView view = new QueryOperationCatalogView(table.getQueryOperation());
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
		return scanInternal(tablePath).map(this::createTable)
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

	@Override
	public String explain(Table table, boolean extended) {
		return planner.explain(Collections.singletonList(table.getQueryOperation()), extended);
	}

	@Override
	public String explain(boolean extended) {
		List<Operation> operations = bufferedModifyOperations.stream()
			.map(o -> (Operation) o).collect(Collectors.toList());
		return planner.explain(operations, extended);
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return planner.getCompletionHints(statement, position);
	}

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
	public JobExecutionResult execute(String jobName) throws Exception {
		//translate(bufferedModifyOperations);
		bufferedModifyOperations.clear();
		return this.executor.execute(jobName);
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

	private TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}

	protected void validateTableSource(TableSource<?> tableSource) {
		TableSourceValidation.validateTableSource(tableSource);
	}

	private void translate(List<ModifyOperation> modifyOperations) {
		List<Transformation<?>> transformations = planner.translate(modifyOperations);
		executor.apply(transformations);
	}

	private void buffer(List<ModifyOperation> modifyOperations) {
		bufferedModifyOperations.addAll(modifyOperations);
	}

	private Optional<CatalogQueryOperation> scanInternal(String... tablePath) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(tablePath);
		return catalogManager.getTable(objectIdentifier)
			.map(t -> new CatalogQueryOperation(objectIdentifier, t.getSchema()));
	}

	private <T> DataSetQueryOperation<T> asQueryOperation(DataSet<T> dataSet, Optional<List<Expression>> fields) {
		TypeInformation<T> dataSetType = dataSet.getType();

		FieldInfoUtils.TypeInfoSchema typeInfoSchema = fields.map(f -> {
			FieldInfoUtils.TypeInfoSchema fieldsInfo = FieldInfoUtils.getFieldsInfo(
				dataSetType,
				f.toArray(new Expression[0]));

			return fieldsInfo;
		}).orElseGet(() -> FieldInfoUtils.getFieldsInfo(dataSetType));

		return new DataSetQueryOperation<T>(
			dataSet,
			typeInfoSchema.getIndices(),
			typeInfoSchema.toTableSchema());
	}
}
