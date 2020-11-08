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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.types.Row;

/**
 *
 */
public class StreamingDatalogEnvironmentImpl extends StreamTableEnvironmentImpl implements StreamingDatalogEnvironment {
	public StreamingDatalogEnvironmentImpl(
		CatalogManager catalogManager,
		ModuleManager moduleManager,
		FunctionCatalog functionCatalog,
		TableConfig config,
		StreamExecutionEnvironment scalaExecutionEnvironment,
		Planner planner,
		Executor executor,
		boolean isStreaming,
		ClassLoader userClassLoader) {
		super(
			catalogManager,
			moduleManager,
			functionCatalog,
			config,
			scalaExecutionEnvironment,
			planner,
			executor,
			isStreaming,
			userClassLoader);
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
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
		return null;
	}
}


