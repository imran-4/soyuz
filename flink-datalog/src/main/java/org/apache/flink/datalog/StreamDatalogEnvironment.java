package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public interface StreamDatalogEnvironment extends DatalogEnvironment, StreamTableEnvironment {



	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static StreamDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		return null;
	}

	void evaluateDatalogRules(String text);

	<T> DataStream<T> datalogQuery(String query);
}
