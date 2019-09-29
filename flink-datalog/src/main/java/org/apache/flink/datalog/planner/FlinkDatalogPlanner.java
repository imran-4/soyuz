package org.apache.flink.datalog.planner;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;

public class FlinkDatalogPlanner implements Planner {

	public List<Operation> parse(String query) {
		return null;
	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		return null;
	}

	@Override
	public String explain(List<Operation> operations, boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return new String[0];
	}

	public Object validate(Object parsed) {
		return null;
	}
}
