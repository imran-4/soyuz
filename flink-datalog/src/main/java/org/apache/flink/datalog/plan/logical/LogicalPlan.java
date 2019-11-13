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
import java.util.List;
import java.util.stream.Collectors;

//IN progress
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

//	private RelNode getIDBNode(PredicateData predicateData) {
//		String[] outputProjection = predicateData
//			.getPredicateParameters()
//			.stream()
//			.map(TermData::getTermName)
//			.toArray(String[]::new);
//		String[] objects = Arrays.stream(outputProjection).map(x -> "").toArray(String[]::new); //todo: fix datatypes based on input data
//		relBuilder
//			.values(outputProjection, objects)
//			.transientScan(predicateData.getPredicateName());
//
//		List<RexNode> projectionParameters = new ArrayList<>();
//		for (TermData termData : predicateData.getPredicateParameters()) {
//			projectionParameters.add(relBuilder.field(termData.getTermName())); //not sure whether to use ordinals, or variables as names.... // also not sure whether relBuilder.field() would push elements on relBuilder stack......
//
//			if (termData.getAdornment() == TermData.Adornment.BOUND) {
//				relBuilder.filter(
//					relBuilder.call(
//						SqlStdOperatorTable.EQUALS,
//						relBuilder.field(termData.getTermName()),
//						relBuilder.literal(termData.getTermName())));
//			}
//		}
//		relBuilder.project(projectionParameters);
//		return relBuilder.build();
//	}

	private RelNode getLeafNode(PredicateData predicateData) {
		if (predicateData instanceof SimplePredicateData) { //this should also return correct logical plan if there is only query and no program rules are provided (i.e., there is only one node in the And-Or tree.)
			String tableName = predicateData.getPredicateName();
			if (((SimplePredicateData) predicateData).isIdb()) {
				return relBuilder.join(JoinRelType.INNER, "").build();
			} else {
				relBuilder.scan(this.currentCatalogName, this.currentDatabaseName, tableName);
				String[] tableFields = new String[0];
				try {
					tableFields = this.currentCatalog.getTable(ObjectPath.fromString(this.currentDatabaseName + "." + tableName)).getSchema().getFieldNames();
				} catch (TableNotExistException e) {
					e.printStackTrace();
				}
				int i = 0;
				List<RexNode> projectionParameters = new ArrayList<>();
				List<String> newNames = new ArrayList<>();
				for (TermData termData : predicateData.getPredicateParameters()) {
					projectionParameters.add(relBuilder.alias(
						relBuilder.field(predicateData.getPredicateName(), tableFields[i]), termData.getTermName()));
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
					newNames.add(termData.getTermName());
					i++;
				}
				relBuilder
					.project(projectionParameters)
					.rename(newNames);
				RelNode leafNodes = relBuilder.build();
				System.out.println(RelOptUtil.toString(leafNodes));
				return leafNodes;
			}
		} else if (predicateData instanceof PrimitivePredicateData) {
			RexNode leftExpression = relBuilder.field(((PrimitivePredicateData) predicateData).getLeftTerm().getTermName());
			RexNode rightExpression = null;
			if (((PrimitivePredicateData) predicateData).getRightTerm().getAdornment() == TermData.Adornment.BOUND) {
				rightExpression = relBuilder.literal(((PrimitivePredicateData) predicateData).getRightTerm().getTermName());
			} else if (((PrimitivePredicateData) predicateData).getRightTerm().getAdornment() == TermData.Adornment.FREE) {
				rightExpression = relBuilder.field(((PrimitivePredicateData) predicateData).getRightTerm().getTermName());
			}
			return relBuilder.filter(
				relBuilder.call(
					getBinaryOperator(((PrimitivePredicateData) predicateData).getOperator()),
					leftExpression,
					rightExpression
				)).build();
		} else {
			return null;
		}
	}

	@Override
	public RelNode visitOrNode(OrNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<AndNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) {
			boolean hasRecursiveNode = false;
			for (AndNode childNode : childNodes) { //todo: handle a case where root node has more than two children e.g. two or more non recursive and one or more recursive.. use union bw non-recursive tables and in this case... dont know what to do if we have two recursive rules in the same program...???????
				if (childNode.isRecursive()) {
					relBuilder
						.transientScan(childNode.getPredicateData().getPredicateName());
					hasRecursiveNode = true;
				}
				RelNode childRelNode = visit(childNode);
				relBuilder
					.push(childRelNode);
			}
			if (hasRecursiveNode) {
				relBuilder
					.repeatUnion(predicateData.getPredicateName(), true); //create repeat union between top two expressions on the stack
			} else {
				relBuilder
					.union(true);
			}
			relBuilder
				.project(this.getIDBProjectionParameters(predicateData));
			RelNode topNodeRelAlgebra = relBuilder
				.build();
			System.out.println(RelOptUtil.toString(topNodeRelAlgebra));
			return topNodeRelAlgebra;
		} else {
			return getLeafNode(predicateData);
		}
	}

	@Override
	public RelNode visitAndNode(AndNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<OrNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) {
			RelNode previousRelNode = null; OrNode previousChildNode = null;
			for (int i = 0; i < node.getChildren().size(); i++) {
				OrNode childNode = (OrNode) node.getChild(i);
				PredicateData bodyPredicateData = childNode.getPredicateData();
				if (bodyPredicateData instanceof PrimitivePredicateData) {
					relBuilder
						.push(visit(childNode));
				} else if (bodyPredicateData instanceof SimplePredicateData) { // use joins, or cartesian products, etc...
					System.out.println();
					if (((SimplePredicateData) bodyPredicateData).isIdb()) {
						//todo: currently the fields are hardcoded.... remove this later
						previousRelNode = relBuilder.join(JoinRelType.INNER, relBuilder.equals(relBuilder.field(1), relBuilder.field(0))).build();
						relBuilder
							.push(previousRelNode);
						continue;
					}
					RelNode childRelNode = visit(childNode);
					if (previousRelNode != null) {
						relBuilder
							.push(childRelNode);

						//find the matching variables in predicate parameters, and then get the corresponding actual column names, form fields and conditions....
						List<String> currentNodeChuldren = childNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
						List<String> previousNodeChildren = previousChildNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
						List<RexNode> conditions = new ArrayList<>();
						for (int c = 0; c < currentNodeChuldren.size(); c++) {
							for (int p = 0; p < previousNodeChildren.size(); p++) {
								if (currentNodeChuldren.get(c).equals(previousNodeChildren.get(p))) {
									conditions.add(relBuilder.equals(
										relBuilder.field(childNode.getPredicateData().getPredicateParameters().get(c).getTermName()),
										relBuilder.field(previousChildNode.getPredicateData().getPredicateParameters().get(c).getTermName())
									));
								}
							}
						}
						previousRelNode = relBuilder.join(JoinRelType.INNER, conditions).build();
					} else {
						previousRelNode = childRelNode;
					}
					relBuilder
						.push(previousRelNode);
					previousChildNode = childNode;
				}
			}
			relBuilder
				.project(this.getIDBProjectionParameters(predicateData));
			RelNode ruleHeadRelAlgebra = relBuilder
				.build();
			System.out.println(RelOptUtil.toString(ruleHeadRelAlgebra));
			return ruleHeadRelAlgebra;
		} else {
			System.err.println("AND node must have children.");
			return null;
		}
	}
}
