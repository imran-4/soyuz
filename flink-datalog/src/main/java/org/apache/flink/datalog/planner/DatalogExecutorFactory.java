package org.apache.flink.datalog.planner;

import org.apache.flink.datalog.planner.delegation.DatalogBatchExecutor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatalogExecutorFactory implements ExecutorFactory {

	public Executor create(Map<String, String> properties, StreamExecutionEnvironment executionEnvironment) {
		if (Boolean.valueOf(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
			return null; //wil implement it later..
		} else {
			return new DatalogBatchExecutor(executionEnvironment);
		}
	}

	@Override
	public Executor create(Map<String, String> properties) {
		return create(properties, StreamExecutionEnvironment.getExecutionEnvironment());
	}

	@Override
	public Map<String, String> requiredContext() {
		DescriptorProperties properties = new DescriptorProperties();
		return properties.asMap();
	}

	@Override
	public List<String> supportedProperties() {
		return Arrays.asList(EnvironmentSettings.STREAMING_MODE, EnvironmentSettings.CLASS_NAME);
	}

	@Override
	public Map<String, String> optionalContext() {
		Map<String, String> context = new HashMap<>();
		context.put(EnvironmentSettings.CLASS_NAME, this.getClass().getCanonicalName());
		return context;
	}
}
