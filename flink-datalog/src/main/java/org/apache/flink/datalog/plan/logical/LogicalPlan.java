package org.apache.flink.datalog.plan.logical;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.AndOrTreeBaseVisitor;
import org.apache.flink.datalog.parser.tree.OrNode;
import org.apache.flink.datalog.parser.tree.predicate.PredicateData;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.TermData;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalPlan extends AndOrTreeBaseVisitor<RelNode> {   //creates logical plan from And-Or Tree
	private FlinkRelBuilder relBuilder;
	private CatalogManager catalogManager;
	private Catalog currentCatalog;
	private String currentCatalogName;
	private String currentDatabaseName;


	public LogicalPlan(FlinkRelBuilder relBuilder, CatalogManager catalogManager) {
		this.relBuilder = relBuilder;
		this.catalogManager = catalogManager;
		this.currentCatalogName = catalogManager.getCurrentCatalog();
		this.currentCatalog = catalogManager.getCatalog(currentCatalogName).get();
		this.currentDatabaseName = this.catalogManager.getCurrentDatabase();
	}

	private static SqlBinaryOperator getBinaryOperator(String operator) {
		switch (operator) {
			case "==":
				return SqlStdOperatorTable.EQUALS;
			case "!=":
				return SqlStdOperatorTable.NOT_EQUALS;
			case ">":
				return SqlStdOperatorTable.GREATER_THAN;
			case "<":
				return SqlStdOperatorTable.LESS_THAN;
			case ">=":
				return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
			case "<=":
				return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
			//todo::::  add more operators...
			default:
				System.out.println("Opereator not recognized.");
				return null;
		}
	}

	private List<RexNode> getIDBProjectionParameters(PredicateData predicateData) {
		List<RexNode> projectionParameters = new ArrayList<>();
		int i = 0;
		for (TermData termData : predicateData.getPredicateParameters()) {
			if (termData.getAdornment() == TermData.Adornment.BOUND) {
				projectionParameters.add(relBuilder.literal(termData.getTermName()));
			} else {
				projectionParameters.add(
					relBuilder
						.field(termData.getTermName()));
			}
			i++;
		}
		return projectionParameters;
	}

	private void getLeafNode(PredicateData predicateData) {
		if (predicateData instanceof SimplePredicateData) {
			String tableName = predicateData.getPredicateName();

			relBuilder.scan(this.currentCatalogName, this.currentDatabaseName, tableName);
			String[] tableFields = new String[0];
			try {
				tableFields = this.currentCatalog.getTable(ObjectPath.fromString(this.currentDatabaseName + "." + tableName)).getSchema().getFieldNames();
			} catch (TableNotExistException e) {
				e.printStackTrace();
			}
			int i = 0;
			List<RexNode> projectionParameters = new ArrayList<>();

			for (TermData termData : predicateData.getPredicateParameters()) {
				projectionParameters.add(relBuilder.alias(relBuilder.field(tableFields[i]), termData.getTermName()));

				if (termData.getAdornment() == TermData.Adornment.BOUND) {
					if (!((SimplePredicateData) predicateData).isIdb()) {
						System.err.println("Unsafe rule!");
					}
					relBuilder.filter(
						relBuilder.call(
							SqlStdOperatorTable.EQUALS,
							relBuilder.field(i),
							relBuilder.literal(termData.getTermName())));
				}
				i++;
			}
			relBuilder
				.project(projectionParameters);
		} else if (predicateData instanceof PrimitivePredicateData) {
			RexNode leftExpression = relBuilder.field(((PrimitivePredicateData) predicateData).getLeftTerm().getTermName());
			RexNode rightExpression = null;
			if (((PrimitivePredicateData) predicateData).getRightTerm().getAdornment() == TermData.Adornment.BOUND) {
				rightExpression = relBuilder.literal(((PrimitivePredicateData) predicateData).getRightTerm().getTermName());
			} else if (((PrimitivePredicateData) predicateData).getRightTerm().getAdornment() == TermData.Adornment.FREE) {
				rightExpression = relBuilder.field(((PrimitivePredicateData) predicateData).getRightTerm().getTermName());
			}
			relBuilder.filter(
				relBuilder.call(
					getBinaryOperator(((PrimitivePredicateData) predicateData).getOperator()),
					leftExpression,
					rightExpression
				));
		}
	}

	public RelNode getLogicalPlan() {
		RelNode relNode = this.relBuilder.build();
		System.out.println(RelOptUtil.toString(relNode));
		return relNode;
	}

	@Override
	public void visitOrNode(OrNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<AndNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) {
			boolean hasRecursiveNode = false;
			int recursiveNodesCount = 0;
			for (int i = 0; i < childNodes.size(); i++) { //todo: handle a case where root node has more than two children e.g. two or more non recursive and one or more recursive.. use union bw non-recursive tables and in this case... dont know what to do if we have two recursive rules in the same program...???????
				AndNode childNode = childNodes.get(i);
				if (childNode.isRecursive()) {
					relBuilder
						.transientScan(childNode.getPredicateData().getPredicateName());
					hasRecursiveNode = true;
					recursiveNodesCount++; //not sure whether we need to union multiple recursuve nodes
				} else if (i >= 1 && !childNode.isRecursive()) { //if there are multiple rules without recursion seen so far. then union their results.. i hope it would be ok..not sure whether repeatunion will create a simple "union" among multiple non recursive nodes..
					relBuilder.union(true);
				}
				visit(childNode);
			}
			relBuilder
				.project(this.getIDBProjectionParameters(predicateData));
			if (hasRecursiveNode) {
				relBuilder
					.repeatUnion(predicateData.getPredicateName(), true); //create repeat union between top two expressions on the stack
			} else if (childNodes.size() > 1){
				relBuilder
					.union(true);
			}
		} else {
			getLeafNode(predicateData);
		}
	}

	@Override
	public void visitAndNode(AndNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<OrNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) {
			for (int i = 0; i < node.getChildren().size(); i++) {
				OrNode childNode = (OrNode) node.getChild(i);
				PredicateData bodyPredicateData = childNode.getPredicateData();
				if (bodyPredicateData instanceof PrimitivePredicateData) {
					visit(childNode);
				} else if (bodyPredicateData instanceof SimplePredicateData) { // use joins, or cartesian products, etc...
					if (!((SimplePredicateData) bodyPredicateData).isIdb()) {
						visit(childNode);
					}
					if (i == 1) {
						OrNode previousChildNode = node.getChildren().get(i - 1);
						//find the matching variables in predicate parameters, and then get the corresponding actual column names, form fields and conditions....
						createJoin(previousChildNode, childNode);
					} else if (i > 1) { // join is required with previous join
						createJoinWithJoin(childNode);

					} else if (i == 0 && ((SimplePredicateData) bodyPredicateData).isIdb()) { //the case where first node is an IDB
						if (i + 1 < node.getChildren().size()) { //take its next sibling and if that's also a simple predicate, then visit it and create a join between the two predicates...
							OrNode nextChildNode = (OrNode) node.getChild(i + 1);
							visit(nextChildNode);
							if (nextChildNode.getPredicateData() instanceof SimplePredicateData) { //else it would be a primitive predicate, which would already be added upon visit.
								//create join here
								createJoin(childNode, nextChildNode);
							}
							i++;
						}
					}
				}
			}
			relBuilder
				.project(this.getIDBProjectionParameters(predicateData));
		} else {
			System.err.println("AND node must have children.");
		}
	}

	private void createJoinWithJoin(OrNode currentNode) {
		//todo::: fix this.....
		List<String> newNames = new ArrayList<>();
		List<RexNode> conditions = new ArrayList<>();

		List<String> currentNodeFields = currentNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
		List<String> joinedNodeFields = relBuilder.peek(1).getRowType().getFieldNames().stream().distinct().collect(Collectors.toList());
		String previousJoinTableName = "join";
		newNames.addAll(currentNodeFields);
		newNames.addAll(joinedNodeFields);

		for (int l = 0; l < joinedNodeFields.size(); l++) {
			for (int r = 0; r < currentNodeFields.size(); r++) {
				if (joinedNodeFields.get(l).equals(currentNodeFields.get(r))) {
					RexNode leftRexNode = null;  //left node cant be IDB in this case..that case has been handeled before
					leftRexNode = relBuilder
						.field(2, previousJoinTableName, joinedNodeFields.get(l));

					RexNode rightRexNode = null;
					if (((SimplePredicateData) currentNode.getPredicateData()).isIdb()) { //as idb can be located far in the  siblining list
						Map<String, Integer> inputOrdinal = getInputCountAndOrdinal(currentNode.getPredicateData().getPredicateName());
						rightRexNode = relBuilder.field(inputOrdinal.get("count"), inputOrdinal.get("ordinal"), r);
					} else {
						rightRexNode = relBuilder
							.field(currentNode.getPredicateData().getPredicateName(), currentNodeFields.get(r));
					}
					conditions.add(relBuilder.equals(leftRexNode, rightRexNode));
				}
			}
		}
		relBuilder.join(JoinRelType.INNER, conditions).rename(newNames).as("join");
	}

	private void createJoin(OrNode leftNode, OrNode rightNode) {
		List<String> leftNodeFields = leftNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
		List<String> rightNodeFields = rightNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
		List<RexNode> conditions = new ArrayList<>();
		List<String> newNames = new ArrayList<>();
		newNames.addAll(leftNodeFields);
		newNames.addAll(rightNodeFields);

		for (int l = 0; l < leftNodeFields.size(); l++) {
			for (int r = 0; r < rightNodeFields.size(); r++) {
				if (leftNodeFields.get(l).equals(rightNodeFields.get(r))) {
					RexNode leftRexNode = null;
					if (((SimplePredicateData) leftNode.getPredicateData()).isIdb()) { //as idb can be located far in the  siblining list
						Map<String, Integer> inputOrdinal = getInputCountAndOrdinal(leftNode.getPredicateData().getPredicateName());
						leftRexNode = relBuilder
							.field(2, 0, l);
					} else {
						leftRexNode = relBuilder
							.field(leftNode.getPredicateData().getPredicateName(), leftNodeFields.get(l));
					}
					RexNode rightRexNode = null;
					if (((SimplePredicateData) rightNode.getPredicateData()).isIdb()) { //as idb can be located far in the  siblining list
						Map<String, Integer> inputOrdinal = getInputCountAndOrdinal(rightNode.getPredicateData().getPredicateName());
						rightRexNode = relBuilder.field(inputOrdinal.get("count"), inputOrdinal.get("ordinal"), r);
					} else {
						rightRexNode = relBuilder
							.field(rightNode.getPredicateData().getPredicateName(), rightNodeFields.get(r));
					}
					conditions.add(relBuilder.equals(leftRexNode, rightRexNode));
				}
			}
		}
		relBuilder.join(JoinRelType.INNER, conditions).rename(newNames).as("join");
	}

	private Map<String, Integer> getInputCountAndOrdinal(String transientTableName) {
		Map<String, Integer> countAndOrdinal = new HashMap<>();
		int peekIndex = 0;
		int inputCount = 0;
		int inputOrdinal = 0;
		while (true) {
			inputCount++;
			try {
				RelNode peekNode = relBuilder.peek(peekIndex);
				List<String> tableFullyQualifiedName = peekNode.getTable().getQualifiedName();
				String tableName = tableFullyQualifiedName.get(tableFullyQualifiedName.size() - 1);
				if (transientTableName.equals(tableName)) {
					inputOrdinal = peekIndex;
					break;
				}
				peekIndex++;
			} catch (IndexOutOfBoundsException e) {
				break;
			} catch (Exception ignored) {
				peekIndex++;
				ignored.printStackTrace();
			}
		}
		countAndOrdinal.put("count", inputCount);
		countAndOrdinal.put("ordinal", inputOrdinal);

		return countAndOrdinal;
	}
}
