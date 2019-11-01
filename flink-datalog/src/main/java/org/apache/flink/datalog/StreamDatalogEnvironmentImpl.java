package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.exceptions.ModuleAlreadyExistException;
import org.apache.flink.table.module.exceptions.ModuleNotFoundException;
import org.apache.flink.table.operations.*;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;

import java.util.*;
import java.util.stream.Collectors;

public class StreamDatalogEnvironmentImpl implements StreamDatalogEnvironment {
	private final OperationTreeBuilder operationTreeBuilder;
	private final FunctionCatalog functionCatalog;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();
	private CatalogManager catalogManager;
	private Executor executor;
	private TableConfig tableConfig;
	private Planner planner;

	public StreamDatalogEnvironmentImpl(Executor executor, TableConfig tableConfig, CatalogManager catalogManager, FunctionCatalog functionCatalog, Planner planner) {
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
			true
		);
	}

	@Override
	public Table datalogQuery(String inputProgram, String query) {
		return null;
	}


	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {

	}

	@Override
	public <T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {

	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream) {
		return null;
	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream, String fields) {
		return null;
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream) {

	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {

	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
		return null;
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
		return null;
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig) {
		return null;
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig) {
		return null;
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
		return null;
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo) {
		return null;
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig) {
		return null;
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig) {
		return null;
	}

	@Override
	public Table fromTableSource(TableSource<?> source) {
		return createTable(new TableSourceQueryOperation<>(source, false));
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
	public void loadModule(String moduleName, Module module) throws ModuleAlreadyExistException {

	}

	@Override
	public void unloadModule(String moduleName) throws ModuleNotFoundException {

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
						false);
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
			ConnectorCatalogTable source = ConnectorCatalogTable.source(tableSource, false);
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

	}

	@Override
	public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return new StreamTableDescriptor(this, connectorDescriptor);
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
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public String[] listFunctions() {
		throw new UnsupportedOperationException("Not supported.");
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
	public void sqlUpdate(String stmt, StreamQueryConfig config) {
		throw new UnsupportedOperationException("");
	}

	@Override
	public void insertInto(Table table, StreamQueryConfig queryConfig, String sinkPath, String... sinkPathContinued) {
		List<String> fullPath = new ArrayList<>(Arrays.asList(sinkPathContinued));
		fullPath.add(0, sinkPath);

		List<ModifyOperation> modifyOperations = Collections.singletonList(
			new CatalogSinkModifyOperation(
				fullPath,
				table.getQueryOperation()));

		if (isEagerOperationTranslation()) {
			translate(modifyOperations);
		} else {
			buffer(modifyOperations);
		}
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
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
						.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, false);
					catalogManager.alterTable(sourceAndSink, getTemporaryObjectIdentifier(name), false);
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, false);
			catalogManager.createTable(sink, getTemporaryObjectIdentifier(name), false);
		}
	}

	Optional<CatalogBaseTable> getCatalogTable(String... name) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(name);
		return catalogManager.getTable(objectIdentifier);
	}

	TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}

	private Optional<CatalogQueryOperation> scanInternal(String... tablePath) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(tablePath);
		return catalogManager.getTable(objectIdentifier)
			.map(t -> new CatalogQueryOperation(objectIdentifier, t.getSchema()));
	}

	private boolean isEagerOperationTranslation() {
		return false;
	}

	private void translate(List<ModifyOperation> modifyOperations) {
		List<Transformation<?>> transformations = planner.translate(modifyOperations);
		executor.apply(transformations);
	}

	private void buffer(List<ModifyOperation> modifyOperations) {
		bufferedModifyOperations.addAll(modifyOperations);
	}
}
