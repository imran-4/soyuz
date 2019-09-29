package org.apache.flink.datalog.planner.delegation;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Executor;

import java.util.List;

public class DatalogBatchExecutor implements Executor {
	@Override
	public void apply(List<Transformation<?>> transformations) {
		
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		return null;
	}
}
