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
import scala.math.Ordering;

import java.util.ArrayList;
import java.util.Arrays;
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

	private RelNode getIDBNode(PredicateData predicateData) {
		String[] outputProjection = predicateData
			.getPredicateParameters()
			.stream()
			.map(TermData::getTermName)
			.toArray(String[]::new);
		String[] objects = Arrays.stream(outputProjection).map(x -> "").toArray(String[]::new); //todo: fix datatypes based on input data
		relBuilder
			.values(outputProjection, objects)
			.transientScan(predicateData.getPredicateName());

		List<RexNode> projectionParameters = new ArrayList<>();
		for (TermData termData : predicateData.getPredicateParameters()) {
			projectionParameters.add(relBuilder.field(termData.getTermName())); //not sure whether to use ordinals, or variables as names.... // also not sure whether relBuilder.field() would push elements on relBuilder stack......

			if (termData.getAdornment() == TermData.Adornment.BOUND) {
				relBuilder.filter(
					relBuilder.call(
						SqlStdOperatorTable.EQUALS,
						relBuilder.field(termData.getTermName()),
						relBuilder.literal(termData.getTermName())));
			}
		}
		relBuilder.project(projectionParameters);
		return relBuilder.build(); //will also clear the stack
	}

	private RelNode getLeafNode(PredicateData predicateData) {
		if (predicateData instanceof SimplePredicateData) { //this should also return correct logical plan if there is only query and no program rules are provided (i.e., there is only one node in the And-Or tree.)
			String tableName = predicateData.getPredicateName();
			System.out.println();
			if (((SimplePredicateData) predicateData).isIdb()) {
//				relBuilder.values(predicateData
//					.getPredicateParameters()
//					.stream()
//					.map(TermData::getTermName)
//					.toArray(String[]::new), null, null);
				relBuilder.transientScan(tableName);

				List<RexNode> projectionParameters = new ArrayList<>();
				for (TermData termData : predicateData.getPredicateParameters()) {
					projectionParameters.add(relBuilder.field(termData.getTermName())); //not sure whether to use ordinals, or variables as names.... // also not sure whether relBuilder.field() would push elements on relBuilder stack......

					if (termData.getAdornment() == TermData.Adornment.BOUND) {
						relBuilder.filter(
							relBuilder.call(
								SqlStdOperatorTable.EQUALS,
								relBuilder.field(termData.getTermName()),
								relBuilder.literal(termData.getTermName())));
					}
				}
				relBuilder.project(projectionParameters);

				return relBuilder.build();
			} else {
				relBuilder.scan(this.currentCatalogName, this.currentDatabaseName, tableName);
				String[] tableFields = new String[0];
				try {
					tableFields = this.currentCatalog.getTable(ObjectPath.fromString(this.currentDatabaseName + "." + tableName)).getSchema().getFieldNames();
				} catch (TableNotExistException e) {
					e.printStackTrace();
				}
				List<RexNode> projectionParameters = new ArrayList<>();
				List<String> newNames = new ArrayList<>();
				int i = 0;
				for (TermData termData : predicateData.getPredicateParameters()) {
					projectionParameters.add(relBuilder.alias(relBuilder.field(tableFields[i]), termData.getTermName())); //not sure whether to use ordinals, or variables as names.... // also not sure whether relBuilder.field() would push elements on relBuilder stack......
					if (termData.getAdornment() == TermData.Adornment.BOUND) {
						relBuilder.filter(
							relBuilder.call(
								SqlStdOperatorTable.EQUALS,
								relBuilder.field(tableFields[i]),
								relBuilder.literal(termData.getTermName())));
					}
					newNames.add(termData.getTermName());
					i++;
				}
				relBuilder.project(projectionParameters).rename(newNames);
				RelNode leafNodes = relBuilder.build(); //will also clear the stack
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
				)).build(); //will also clear the stack
		} else {
			return null;
		}
	}

	@Override
	public RelNode visitOrNode(OrNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<AndNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) {
//			RelNode idbNode = getIDBNode(predicateData);
//			relBuilder.push(idbNode);
			AndNode preivousSiblingNode = null;
			for (AndNode childNode : childNodes) { //todo: handle a case where root node has more than two children e.g. two or more non recursive and one or more recursive.. use union bw non-recursive tables and in this case... dont know what to do if we have two recursive rules in the same program...???????
				RelNode childRelNode = visit(childNode); //child node is AND node
				relBuilder.push(childRelNode); //not sure whether to put in relbuilder stack or create our own stack....

				if (preivousSiblingNode != null) {
					if (!preivousSiblingNode.isRecursive() && childNode.isRecursive()) {//todo: dont know if vice versa is also true. //if the previous rule wasnt recursive and the current one is recursive.
						relBuilder.repeatUnion(predicateData.getPredicateName(), true); //create repeat union between top two expressions on the stack
					} else { //if ((!preivousSiblingNode.isRecursive() && !andNode.isRecursive()) || (preivousSiblingNode.isRecursive() && andNode.isRecursive())) { //if both are not recursive or if both are recursive.. //todo: not sure if both are recursive
						relBuilder.union(true);
					}
				}
				preivousSiblingNode = childNode;
			}

			RelNode topNodeRelAlgebra = relBuilder.build();
			System.out.println(RelOptUtil.toString(topNodeRelAlgebra));
			return topNodeRelAlgebra;
		} else { //leaf nodes
			return getLeafNode(predicateData);
		}
	}

	@Override
	public RelNode visitAndNode(AndNode node) {
		PredicateData predicateData = node.getPredicateData();
		List<OrNode> childNodes = node.getChildren();
		if (childNodes.size() > 0) { //this case will be always true. but checking  it anyway....
			RelNode idbNode = getIDBNode(predicateData);
			relBuilder.push(idbNode);
			RelNode previousRelNode = null;
			for (OrNode childNode : node.getChildren()) {
				RelNode childRelNode = visit(childNode); //child node is OR node

				PredicateData bodyPredicateData = childNode.getPredicateData();
				if (bodyPredicateData instanceof PrimitivePredicateData) {
					relBuilder.push(childRelNode);// it is a filter
				} else if (bodyPredicateData instanceof SimplePredicateData) { // use joins, or cartesian products, etc...
					if (previousRelNode != null) {
						relBuilder.push(childRelNode);
						List<String> matchingColumns = getMatchingColumns(previousRelNode.getRowType().getFieldNames(), childRelNode.getRowType().getFieldNames());
						previousRelNode = relBuilder.join(JoinRelType.INNER, matchingColumns.toArray(new String[0])).build(); //todo:  check what to do if there is no matching parameter.....
					} else {
						previousRelNode = childRelNode;
					}
					relBuilder.push(previousRelNode);
				}
			}

			relBuilder.project(relBuilder.fields(predicateData.getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList())));//.as(predicateData.getPredicateName());
			relBuilder.repeatUnion(predicateData.getPredicateName(), true);

			RelNode ruleHeadRelAlgebra = relBuilder.build();
			System.out.println(RelOptUtil.toString(ruleHeadRelAlgebra));
			return ruleHeadRelAlgebra; //since it's a transient scan, it means it's not stored in the catalog, and hence we don't need catalog name and database name
		} else {
			System.err.println("AND node must have children.");
			return null;
		}
	}
}
