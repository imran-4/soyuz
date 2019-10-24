package org.apache.flink.datalog.planner.calcite;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;

public class FlinkDatalogContextImpl extends FlinkContextImpl implements FlinkDatalogContext {

	private ExecutionEnvironment executionEnvironment;
	public FlinkDatalogContextImpl(TableConfig tableConfig, FunctionCatalog functionCatalog, ExecutionEnvironment executionEnvironment) {
		super(tableConfig, functionCatalog);
		this.executionEnvironment = executionEnvironment;
	}

	@Override
	public ExecutionEnvironment getExecutionEnvironment() {
		return this.executionEnvironment;
	}
}
