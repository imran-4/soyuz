package org.apache.flink.datalog.executor;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.delegation.Executor;

import java.util.List;

public class DatalogStreamExecutor implements Executor {
	private final StreamExecutionEnvironment executionEnvironment;

	public DatalogStreamExecutor(StreamExecutionEnvironment executionEnvironment) {
		this.executionEnvironment = executionEnvironment;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}


	@Override
	public void apply(List<Transformation<?>> transformations) {
		transformations.forEach(executionEnvironment::addOperator);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		return executionEnvironment.execute(jobName);
	}

}
