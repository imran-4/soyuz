package org.apache.flink.datalog.planner.calcite;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.planner.calcite.FlinkContext;

public interface FlinkDatalogContext extends FlinkContext {
	ExecutionEnvironment getExecutionEnvironment();

}
