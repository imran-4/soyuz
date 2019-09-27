package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BatchDatalogEnvironmentImpl implements BatchDatalogEnvironment {


	private CatalogManager catalogManager;

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
			if (isCatalogPresent)
				for (String database : this.catalogManager.getCatalog(catalog).get().listDatabases()) {
					try {
						tables.addAll(this.catalogManager.getCatalog(catalog).get().listTables(database));
					} catch (DatabaseNotExistException e) {
						e.printStackTrace();
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

	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {

	}

	@Override
	public void registerTable(String name, Table table) {

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
		return new String[0];
	}

	@Override
	public String[] listFunctions() {
		return new String[0];
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
		return null;
	}

	@Override
	public void sqlUpdate(String stmt) {

	}

	@Override
	public String getCurrentCatalog() {
		return null;
	}

	@Override
	public void useCatalog(String catalogName) {

	}

	@Override
	public String getCurrentDatabase() {
		return null;
	}

	@Override
	public void useDatabase(String databaseName) {

	}

	@Override
	public TableConfig getConfig() {
		return null;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		return null;
	}

	@Override
	public Object compile(String text) {
		return null;
	}

	@Override
	public Object query(String queryText) {
		return null;
	}
}
