package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.datalog.catalog.DatalogCatalog;
import org.apache.flink.datalog.planner.FlinkDatalogPlanner;
import org.apache.flink.datalog.planner.delegation.DatalogBatchExecutor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;

import java.lang.reflect.Constructor;

public interface BatchDatalogEnvironment extends DatalogEnvironment, BatchTableEnvironment {
	ExecutionEnvironment env = null;

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		try {
			Class<?> clazz = Class.forName("org.apache.flink.datalog.BatchDatalogEnvironmentImpl");
			Constructor con = clazz.getConstructor(DatalogBatchExecutor.class, TableConfig.class, CatalogManager.class, FunctionCatalog.class, FlinkDatalogPlanner.class);
			Executor executor = new DatalogBatchExecutor(executionEnvironment);
			String defaultCatalog = "default_catalog";
			CatalogManager catalogManager = new CatalogManager(
				defaultCatalog,
				new DatalogCatalog(defaultCatalog, "default_database")
			);
			FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
			FlinkDatalogPlanner planner = new FlinkDatalogPlanner();
			return (BatchDatalogEnvironment) con.newInstance(executor, tableConfig, catalogManager, functionCatalog, planner);
		} catch (Throwable t) {
			throw new TableException("Create BatchTableEnvironment failed.", t);
		}
	}

	DataSet datalogRules(String program);

}
