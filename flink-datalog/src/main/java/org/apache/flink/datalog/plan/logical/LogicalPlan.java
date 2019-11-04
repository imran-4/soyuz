package org.apache.flink.datalog.plan.logical;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.OrNode;
import org.apache.flink.datalog.parser.tree.predicate.PredicateData;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.TermData;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.plan.nodes.dataset.DataSetRel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

//IN progress
public class LogicalPlan extends AndOrTreeBaseVisitor<RelNode> {   //creates logical plan from And-Or Tree
	private FlinkRelBuilder relBuilder;

	public LogicalPlan(FlinkRelBuilder relBuilder) {
		this.relBuilder = relBuilder;

	}

	private static List<String> getMatchingColumns(List<String> previousSibling, List<String> currentSibling) {
		return previousSibling.stream()
			.distinct()
			.filter(currentSibling::contains).collect(Collectors.toList());
	}

	private static SqlBinaryOperator getBinaryOperator(String operator) {
		switch (operator) {
			case "==":
				return SqlStdOperatorTable.EQUALS;
			case "!=":
				return SqlStdOperatorTable.NOT_EQUALS;
			case ">":
				return SqlStdOperatorTable.GREATER_THAN;
			default:
				System.out.println("Opereator not recognized.");
				return null;

		}
	}

	@Override
	public RelNode visitOrNode(OrNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<AndNode> childNodes = node.getChildren(); ///or use visitChildren()
		if (childNodes.size() > 0) {
			for (AndNode andNode : childNodes) {
				relBuilder.push(visit(andNode));
			}
			if (node.isRecursive()) {
				relBuilder.repeatUnion(node.getPredicateData().getPredicateName(), true);
			} else {
				relBuilder.union(true);
			}
			return relBuilder.build();
		} else {
			if (predicateData instanceof SimplePredicateData) { //this should also return correct logical plan if there is only query and no program rules are provided (i.e., there is only one node in the And-Or tree.)
				relBuilder.scan(predicateData.getPredicateName());
				List<RexNode> fieldsToProject = new ArrayList<>();
				List<RexNode> filterNodes = new ArrayList<>();
				int i = 0;
				for (TermData termData : predicateData.getPredicateParameters()) {
					if (termData.getAdornment() == TermData.Adornment.BOUND) {

					}
					fieldsToProject.add(relBuilder.field(i));
					i++;
				}
				return relBuilder.project(fieldsToProject).filter(filterNodes).build();
			} else if (predicateData instanceof PrimitivePredicateData) {
				//todo: check if filter() works before scan()
				return relBuilder.filter(relBuilder.call(getBinaryOperator(((PrimitivePredicateData) predicateData).getOperator()),
					relBuilder.field(((PrimitivePredicateData) predicateData).getLeftTerm().getTermName()),
					relBuilder.field(((PrimitivePredicateData) predicateData).getLeftTerm().getTermName()))
				).build();
			} else {
				return null;
			}
		}
	}

	@Override
	public RelNode visitAndNode(AndNode andNode) {
		RelNode previousRelNode = null;
		if (andNode.getChildren().size() > 0) { //this case will be always true. but checking  it anyway....
			for (OrNode orNode : andNode.getChildren()) {
				RelNode node = visit(orNode);
				PredicateData predicateData = orNode.getPredicateData();
				if (predicateData instanceof PrimitivePredicateData) {
					relBuilder.push(node);
				} else if (predicateData instanceof SimplePredicateData) {
					if (previousRelNode != null) {
						List<String> matchingColumns = getMatchingColumns(previousRelNode.getRowType().getFieldNames(), node.getRowType().getFieldNames());
						previousRelNode = relBuilder.join(JoinRelType.INNER, matchingColumns.toArray(new String[0])).build();
					} else {
						previousRelNode = node;
					}
					relBuilder.push(previousRelNode);
				}
			}
			// todo: handle bounded predicate values here..
			String andPredicateName = andNode.getPredicateData().getPredicateName();
			List<TermData> andPredicateParameters = andNode.getPredicateData().getPredicateParameters();
			for (TermData parameter : andPredicateParameters) {

			}
			return relBuilder.transientScan(andPredicateName).project().filter().build();
		} else {
			System.err.println("AND node must have children.");
		}
		return null;
	}
}
