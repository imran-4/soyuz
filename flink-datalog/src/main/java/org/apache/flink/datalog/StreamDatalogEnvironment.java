package org.apache.flink.datalog;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public interface StreamDatalogEnvironment extends DatalogEnvironment, StreamTableEnvironment {

	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		return null;
	}


	Table datalogQuery(String inputProgram, String query);
}
