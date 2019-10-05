package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.datalog.catalog.DatalogCatalog;
import org.apache.flink.datalog.planner.FlinkDatalogPlanner;
import org.apache.flink.datalog.planner.delegation.DatalogBatchExecutor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;

import java.lang.reflect.Constructor;

public interface BatchDatalogEnvironment extends DatalogEnvironment, BatchTableEnvironment {
	ExecutionEnvironment env = null;

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, EnvironmentSettings environmentSettings) {
		return create(executionEnvironment, environmentSettings, new TableConfig());
	}

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, EnvironmentSettings.newInstance().build());
	}

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, EnvironmentSettings environmentSettings, TableConfig tableConfig) {
		return BatchDatalogEnvironmentImpl.create(executionEnvironment, environmentSettings, tableConfig);

	}

	/*
	*
	* */
	void evaluateDatalogRules(String program);

	<T>DataSet<T> datalogQuery(String query);
}
