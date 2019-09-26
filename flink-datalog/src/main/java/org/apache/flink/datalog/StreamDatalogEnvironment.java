package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Optional;

public class StreamDatalogEnvironment extends DatalogEnvironment {

	@Override
	public Table fromTableSource(TableSource<?> source) {
		return null;
	}

	@Override
	public void registerCatalog(String catalogName, Catalog catalog) {

	}

	@Override
	public Optional<Catalog> getCatalog(String catalogName) {
		return Optional.empty();
	}

	@Override
	public void registerFunction(String name, ScalarFunction function) {

	}

	@Override
	public void registerTable(String name, Table table) {

	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {

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
	public String[] listCatalogs() {
		return new String[0];
	}

	@Override
	public String[] listDatabases() {
		return new String[0];
	}

	@Override
	public String[] listTables() {
		return new String[0];
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
	Object compile(String text) {
		return null;
	}

	@Override
	Object query(String queryText) {
		return null;
	}
}
