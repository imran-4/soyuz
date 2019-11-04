package org.apache.flink.datalog.plan.logical;

import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.OrNode;
import org.apache.flink.datalog.parser.tree.predicate.PredicateData;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.QueryPredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.plan.nodes.FlinkRelNode;

//IN progress
public class LogicalPlan extends AndOrTreeBaseVisitor<FlinkRelNode> {   //creates logical plan from And-Or Tree
	private FlinkRelBuilder relBuilder;

	public LogicalPlan(FlinkRelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	@Override
	public FlinkRelNode visitOrNode(OrNode node) {
		if (node.getChildren().size() == 0) {
			PredicateData predicateData = node.getPredicateData();
			if (predicateData instanceof SimplePredicateData) {
				if (predicateData instanceof QueryPredicateData) {
					relBuilder.repeatUnion(predicateData.getPredicateName(), true);
				} else {
					relBuilder.scan(predicateData.getPredicateName());
				}
			} else if (predicateData instanceof PrimitivePredicateData) {
				System.out.println();
			}
		} else {
			if (node.isRecursive()) {
				relBuilder.repeatUnion(node.getPredicateData().getPredicateName(), true);
			}
			visit(node.getChildren().get(0));
		}
		return null;
	}

	@Override
	public FlinkRelNode visitAndNode(AndNode node) {
		if (node.getChildren().size() != 0) {
			visit(node.getChildren().get(0));
		}
		return null;
	}


}
