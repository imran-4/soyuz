package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public interface BatchDatalogEnvironment extends DatalogEnvironment, BatchTableEnvironment {
//	ExecutionEnvironment env = null;
//
//	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
//		return create(executionEnvironment, new TableConfig());
//	}
//
//	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
//		return null;
//	}

	DataSet compile(String program);

}
