package org.apache.flink.datalog.plan.logical;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.table.calcite.FlinkRelBuilder;

public class LogicalPlan {
	private FlinkRelBuilder relBuilder;
	private Node rootNode;

	public LogicalPlan(FlinkRelBuilder relBuilder, Node rootNode) {
		this.relBuilder = relBuilder;
		this.rootNode = rootNode;
	}


}
