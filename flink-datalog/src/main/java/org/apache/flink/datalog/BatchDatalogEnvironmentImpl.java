package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.datalog.planner.FlinkDatalogPlanner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BatchDatalogEnvironmentImpl<T> implements BatchDatalogEnvironment {

	private CatalogManager catalogManager;
	private ExecutionEnvironment executionEnvironment;
	private TableConfig tableConfig;

	public BatchDatalogEnvironmentImpl() {
		this.executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
		this.tableConfig = new TableConfig();
	}

	public BatchDatalogEnvironmentImpl(ExecutionEnvironment executionEnvironment) {
		this.executionEnvironment = executionEnvironment;
		this.tableConfig = new TableConfig();
	}

	public BatchDatalogEnvironmentImpl(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		this.executionEnvironment = executionEnvironment;
		this.tableConfig = tableConfig;

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
	public Table fromTableSource(TableSource<?> source) {

		return null;
	}


	@Override
	public void registerFunction(String name, ScalarFunction function) {
		throw new UnsupportedOperationException("Not supported");
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
	public void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {

	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {

	}

	@Override
	public Table scan(String... tablePath) {
		return null;
	}

	@Override
	public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {

	}

	@Override
	public ConnectTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return null;
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
		return null;
	}

	@Override
	public String explain(Table table, boolean extended) {
		return null;
	}

	@Override
	public String explain(boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return new String[0];
	}

	@Override
	public Table sqlQuery(String query) {
		throw new UnsupportedOperationException("This implementation is only for Datalog queries, not SQL queries. Use Table API for SQL queries. You are welcome!");
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
		return executionEnvironment.execute(jobName);
	}

	@Override
	public DataSet<T> compile(String text) {
		ParserManager parserManager = new ParserManager();
		parserManager.parseCompileUnit(text);

		//todo: use planner here
		//todo: create AST and logical plans..
		return null;
	}

	@Override
	public void loadDatabase(String text) {
		Catalog catalog = new DatalogCatalog();
//		catalog.createDatabase("",  new CatalogDatabaseImpl(),false);

		// get parsed data from the listener
		// create a catalog and register tables there
		this.registerCatalog("datalog", catalog);
	}

	@Override
	public void datalogQuery(String query) {

		FlinkDatalogPlanner planner = getFlinkDatalogPlanner();

		Object parsed = planner.parse(query); //in this method, either do implementation using visitor or listener

//		if (null != parsed && parsed.getKind.belongsTo(ParsableTypes.QUERY)) {
//			Object validated = planner.validate(parsed);
//
////			val relational = planner.rel(validated) //or ast
////			createTable(new PlannerQueryOperation(relational.rel))
//		} else {
//			throw new UnsupportedOperationException("");
//		}
	}

	@Override
	public void clearState() {
		this.catalogManager = null;
		this.executionEnvironment = null;
		this.tableConfig = null;
	}


	private FlinkDatalogPlanner getFlinkDatalogPlanner() {
		return null;
	}
}
