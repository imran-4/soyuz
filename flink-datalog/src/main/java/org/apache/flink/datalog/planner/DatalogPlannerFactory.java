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

package org.apache.flink.datalog.planner;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DatalogPlannerFactory implements PlannerFactory {


	@Override
	public Planner create(Map<String, String> properties, Executor executor, TableConfig tableConfig, FunctionCatalog functionCatalog, CatalogManager catalogManager) {

		if (Boolean.parseBoolean(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
			return new FlinkBatchDatalogPlanner(executor, tableConfig, functionCatalog, catalogManager, true); //later may be we can separate the planner for stream and batch.
		} else {
			return new FlinkBatchDatalogPlanner(executor, tableConfig, functionCatalog, catalogManager, false);
		}
	}

	public Map<String, String> optionalContext() {
		Map<String, String> map = new HashMap<>();
		map.put(EnvironmentSettings.CLASS_NAME, this.getClass().getCanonicalName());
		return map;
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
}
