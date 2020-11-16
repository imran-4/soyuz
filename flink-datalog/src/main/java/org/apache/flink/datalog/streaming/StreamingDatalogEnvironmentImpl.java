/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.streaming;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.AbstractDataType;

import java.util.Optional;

public class StreamingDatalogEnvironmentImpl implements StreamingDatalogEnvironment, StreamTableEnvironment {
	public StreamingDatalogEnvironmentImpl(
		CatalogManager catalogManager,
		ModuleManager moduleManager,
		FunctionCatalog functionCatalog,
		TableConfig config,
		StreamExecutionEnvironment executionEnvironment,
		Planner planner,
		Executor executor,
		boolean isStreaming,
		ClassLoader userClassLoader) {
	}


	@Override
	public Table datalogQuery(String inputProgram, String query) {
		return null;
	}

	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {

	}

	@Override
	public <T, ACC> void registerFunction(
		String name,
		AggregateFunction<T, ACC> aggregateFunction) {

	}

	@Override
	public <T, ACC> void registerFunction(
		String name,
		TableAggregateFunction<T, ACC> tableAggregateFunction) {

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
	public <T> Table fromDataStream(DataStream<T> dataStream, Expression... fields) {
		return null;
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream) {

	}

	@Override
	public <T> void createTemporaryView(String path, DataStream<T> dataStream) {

	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {

	}

	@Override
	public <T> void createTemporaryView(String path, DataStream<T> dataStream, String fields) {

	}

	@Override
	public <T> void createTemporaryView(
		String path,
		DataStream<T> dataStream,
		Expression... fields) {

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
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
		return null;
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
		Table table,
		TypeInformation<T> typeInfo) {
		return null;
	}

	@Override
	public Table fromValues(Expression... values) {
		return null;
	}

	@Override
	public Table fromValues(AbstractDataType<?> rowType, Expression... values) {
		return null;
	}

	@Override
	public Table fromValues(Iterable<?> values) {
		return null;
	}

	@Override
	public Table fromValues(AbstractDataType<?> rowType, Iterable<?> values) {
		return null;
	}

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
	public void loadModule(String moduleName, Module module) {

	}

	@Override
	public void unloadModule(String moduleName) {

	}

	@Override
	public void registerFunction(String name, ScalarFunction function) {

	}

	@Override
	public void createTemporarySystemFunction(
		String name,
		Class<? extends UserDefinedFunction> functionClass) {

	}

	@Override
	public void createTemporarySystemFunction(String name, UserDefinedFunction functionInstance) {

	}

	@Override
	public boolean dropTemporarySystemFunction(String name) {
		return false;
	}

	@Override
	public void createFunction(String path, Class<? extends UserDefinedFunction> functionClass) {

	}

	@Override
	public void createFunction(
		String path,
		Class<? extends UserDefinedFunction> functionClass,
		boolean ignoreIfExists) {

	}

	@Override
	public boolean dropFunction(String path) {
		return false;
	}

	@Override
	public void createTemporaryFunction(
		String path,
		Class<? extends UserDefinedFunction> functionClass) {

	}

	@Override
	public void createTemporaryFunction(String path, UserDefinedFunction functionInstance) {

	}

	@Override
	public boolean dropTemporaryFunction(String path) {
		return false;
	}

	@Override
	public void registerTable(String name, Table table) {

	}

	@Override
	public void createTemporaryView(String path, Table view) {

	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {

	}

	@Override
	public void registerTableSink(
		String name,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		TableSink<?> tableSink) {

	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {

	}

	@Override
	public Table scan(String... tablePath) {
		return null;
	}

	@Override
	public Table from(String path) {
		return null;
	}

	@Override
	public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {

	}

	@Override
	public void insertInto(String targetPath, Table table) {

	}

	@Override
	public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return null;
	}

	@Override
	public String[] listCatalogs() {
		return new String[0];
	}

	@Override
	public String[] listModules() {
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
	public String[] listViews() {
		return new String[0];
	}

	@Override
	public String[] listTemporaryTables() {
		return new String[0];
	}

	@Override
	public String[] listTemporaryViews() {
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
	public boolean dropTemporaryTable(String path) {
		return false;
	}

	@Override
	public boolean dropTemporaryView(String path) {
		return false;
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
	public String explainSql(String statement, ExplainDetail... extraDetails) {
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
	public TableResult executeSql(String statement) {
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
	public StatementSet createStatementSet() {
		return null;
	}
}


