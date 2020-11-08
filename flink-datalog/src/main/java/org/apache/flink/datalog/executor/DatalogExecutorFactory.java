/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.executor;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.planner.delegation.BatchExecutor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DatalogExecutorFactory implements ExecutorFactory {

	public Executor create(Map<String, String> properties, StreamExecutionEnvironment executionEnvironment) {
		if (Boolean.parseBoolean(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
			return null;
		} else {
			return new BatchExecutor(executionEnvironment);
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
