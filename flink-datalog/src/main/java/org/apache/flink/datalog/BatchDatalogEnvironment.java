package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;

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
