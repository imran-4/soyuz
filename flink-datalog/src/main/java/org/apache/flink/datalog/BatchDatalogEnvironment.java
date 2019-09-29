package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;

public interface BatchDatalogEnvironment<T> extends DatalogEnvironment {
	ExecutionEnvironment env = null;

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		return null;
	}

	DataSet<T> compile(String program);

}
