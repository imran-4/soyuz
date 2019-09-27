package org.apache.flink.datalog;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;

public interface StreamDatalogEnvironment extends DatalogEnvironment {
	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		return null;
	}

}
