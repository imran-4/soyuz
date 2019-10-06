package org.apache.flink.datalog.planner;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.*;

public class DatalogPlannerFactory implements PlannerFactory {


	@Override
	public Planner create(Map<String, String> properties, Executor executor, TableConfig tableConfig, FunctionCatalog functionCatalog, CatalogManager catalogManager) {

		if (Boolean.valueOf(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
			return new FlinkDatalogPlanner(executor, tableConfig, functionCatalog, catalogManager, true); //later may be we can separate the planner for stream and batch.
		} else {
			return new FlinkDatalogPlanner(executor, tableConfig, functionCatalog, catalogManager, false);
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
