/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.plan.logical;

import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.AndOrTreeBaseVisitor;
import org.apache.flink.datalog.parser.tree.OrNode;
import org.apache.flink.datalog.parser.tree.predicate.PredicateData;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.TermData;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.CatalogManager;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class traverses {@link org.apache.flink.datalog.parser.tree.AndOrTree} using visitors and create a logical plan for a Datalog query.
 * Use {@link #getLogicalPlan()} to obtain the {@link RelNode} for a logical plan.
 */
public class LogicalPlan extends AndOrTreeBaseVisitor<RelNode> {
    private static int stackSize = 0;
    private FlinkRelBuilder relBuilder;
    private String currentCatalogName;
    private String currentDatabaseName;
    private Map<String, Integer> idbNameIdMapping = new HashMap<String, Integer>();

    public LogicalPlan(FlinkRelBuilder relBuilder, CatalogManager catalogManager) {
        this.relBuilder = relBuilder;
        this.currentCatalogName = catalogManager.getCurrentCatalog();
        this.currentDatabaseName = catalogManager.getCurrentDatabase();
    }

    private static SqlBinaryOperator getBinaryOperator(String operator) {
        switch (operator) {
            case "=":
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

    /**
     * @return
     */
    public RelNode getLogicalPlan() {
        RelNode relNode = this.relBuilder.build();
        System.out.println(RelOptUtil.toString(relNode));
        return relNode;
    }

    private List<RexNode> getIDBProjectionParameters(PredicateData predicateData) {
        List<RexNode> projectionParameters = new ArrayList<>();
        for (TermData termData : predicateData.getPredicateParameters()) {
            if (termData.getAdornment() == TermData.Adornment.BOUND) {
                projectionParameters.add(relBuilder.literal(termData.getTermName()));
            } else {
                projectionParameters.add(
                        relBuilder
                                .field(termData.getTermName()));
            }
        }
        return projectionParameters;
    }

    private RexNode getPrimitivePredExpression(TermData termData) {
        if (termData.getAdornment() == TermData.Adornment.BOUND) {
            return relBuilder.literal(termData.getTermName());
        } else {
            return relBuilder.field(termData.getTermName());
        }
    }

    private void getLeafNode(PredicateData predicateData) {
        if (predicateData instanceof SimplePredicateData) {
            String tableName = predicateData.getPredicateName();
            if (((SimplePredicateData) predicateData).isIdb()) {
                return;
            } else {
                relBuilder.scan(this.currentCatalogName, this.currentDatabaseName, tableName);
            }
            int i = 0;
            List<RexNode> projectionParameters = new ArrayList<>();
            List<String> newNames = new ArrayList<>();
            for (TermData termData : predicateData.getPredicateParameters()) {
                projectionParameters.add(relBuilder.alias(relBuilder.field(i), termData.getTermName()));
                if (termData.getAdornment() == TermData.Adornment.BOUND) {
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
        } else if (predicateData instanceof PrimitivePredicateData) {
            PrimitivePredicateData primitivePredicateData = ((PrimitivePredicateData) predicateData);
            RexNode leftExpression = getPrimitivePredExpression(primitivePredicateData.getLeftTerm());
            RexNode rightExpression = getPrimitivePredExpression(primitivePredicateData.getRightTerm());
            relBuilder.filter(
                    relBuilder.call(
                            getBinaryOperator(primitivePredicateData.getOperator()),
                            leftExpression,
                            rightExpression
                    ));
        }
    }

    /**
     * @param node
     */
    @Override
    public void visitOrNode(OrNode node) {
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
                visit(childNode);
            }
//            relBuilder
//                    .project(this.getIDBProjectionParameters(predicateData));
            relBuilder.rename(node.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList()));
            idbNameIdMapping.put(predicateData.getPredicateName(), relBuilder.peek().getId());
            if (hasRecursiveNode) {
                relBuilder
                        .repeatUnion(predicateData.getPredicateName(), true); //create repeat union between top two expressions on the stack
            } else if (childNodes.size() > 1) {
                relBuilder
                        .union(true);
            }
        } else {
            getLeafNode(predicateData);
        }
    }

    /**
     * @param node
     * @throws RuntimeException
     */
    @Override
    public void visitAndNode(AndNode node) throws RuntimeException {
        PredicateData predicateData = node.getPredicateData();
        List<OrNode> childNodes = node.getChildren();
        if (childNodes.size() > 0) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                OrNode childNode = (OrNode) node.getChild(i);
                PredicateData bodyPredicateData = childNode.getPredicateData();
                if (bodyPredicateData instanceof PrimitivePredicateData) {
                    visit(childNode);
                } else if (bodyPredicateData instanceof SimplePredicateData) { // use joins, or cartesian products, etc...
//                    if (!((SimplePredicateData) bodyPredicateData).isIdb()) {
                    visit(childNode);
//                    }
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
                    .project(this.getIDBProjectionParameters(predicateData))
                    .as(predicateData.getPredicateName());
            idbNameIdMapping.put(predicateData.getPredicateName(), relBuilder.peek().getId());
        } else {
            throw new RuntimeException("And node must have children.");
        }
    }

    //todo: refactor the code(lots of redundant code)...
    private void createJoinWithJoin(OrNode currentNode) {
        List<String> newNames = new ArrayList<>();
        List<RexNode> conditions = new ArrayList<>();

        List<String> currentNodeFields = getFieldNames(currentNode);
        List<String> joinedNodeFields = relBuilder.peek(1).getRowType().getFieldNames().stream().distinct().collect(Collectors.toList());
        newNames.addAll(currentNodeFields);
        newNames.addAll(joinedNodeFields);

        for (int l = 0; l < joinedNodeFields.size(); l++) {
            for (int r = 0; r < currentNodeFields.size(); r++) {
                if (joinedNodeFields.get(l).equals(currentNodeFields.get(r))) {
                    RexNode leftRexNode = relBuilder.field(2, 0, joinedNodeFields.get(l)); //peek=1, fields(peek+1). same.
                    RexNode rightRexNode = null;
                    if (((SimplePredicateData) currentNode.getPredicateData()).isIdb()) { //as idb can be located far in the  siblining list
                        rightRexNode = getField(getPredicateName(currentNode), r);
                    } else {
                        rightRexNode = relBuilder
                                .field(1, getPredicateName(currentNode), currentNodeFields.get(r));
                    }
                    conditions.add(relBuilder.equals(leftRexNode, rightRexNode));
                }
            }
        }
        relBuilder
                .join(JoinRelType.INNER, conditions)
                .rename(newNames);
    }

    private void createJoin(OrNode leftNode, OrNode rightNode) { //should take only rightNode, and get leftNde from relBuilder's stack
        String leftPredicateName = getPredicateName(leftNode);
        String rightPredicateName = getPredicateName(rightNode);
        List<String> leftNodeFields = getFieldNames(leftNode);
        List<String> rightNodeFields = getFieldNames(rightNode);
        List<RexNode> conditions = new ArrayList<>();

        List<String> newNames = new ArrayList<>();
        newNames.addAll(leftNodeFields);
        newNames.addAll(rightNodeFields);

        for (int i = 0; i < leftNodeFields.size(); i++) {
            for (int j = 0; j < rightNodeFields.size(); j++) {
                if (leftNodeFields.get(i).equals(rightNodeFields.get(j))) {
                    RexNode leftRexNode, rightRexNode;
                    if (((SimplePredicateData) leftNode.getPredicateData()).isIdb()) {
                        leftRexNode = getField(leftPredicateName, i);
                    } else {
                        leftRexNode = getField(leftPredicateName, leftNodeFields.get(i));
                    }
                    if (((SimplePredicateData) rightNode.getPredicateData()).isIdb()) {
                        rightRexNode = getField(rightPredicateName, j);
                    } else {
                        rightRexNode = getField(rightPredicateName, rightNodeFields.get(j));
                    }
                    conditions.add(relBuilder.equals(leftRexNode, rightRexNode));
                }
            }
        }
        relBuilder
                .join(JoinRelType.INNER, conditions)
                .rename(newNames);
    }

    private RexNode getField(String predicateName, int fieldOrdinal) {
        for (int x = 0; ; x++) {
            if (relBuilder.peek(x).getId() == idbNameIdMapping.get(predicateName)) {
                return relBuilder.field(x + 1, 0, fieldOrdinal); //not sure about input ordinal (i.e. 2nd parameter)
            }
        }
    }

    private RexNode getField(String predicateName, String fieldName) {
        for (int x = 0; ; x++) {
            List<RelOptTable> matchingTables = getTablesAtPosition(predicateName, x);
            for (var t : matchingTables) {
                if (relBuilder.peek(x).getRowType().getFieldNames().contains(fieldName)) {
                    return relBuilder.field(x + 1, predicateName, fieldName);
                }
            }
        }
    }

    private List<RelOptTable> getTablesAtPosition(String predicateName, int x) {
        return RelOptUtil
                .findTables(relBuilder.peek(x))
                .stream()
                .filter(table -> table.getQualifiedName().get(table.getQualifiedName().size() - 1).equals(predicateName))
                .collect(Collectors.toList());
    }

    private String getPredicateName(OrNode orNode) {
        return orNode.getPredicateData().getPredicateName();
    }

    private List<String> getFieldNames(OrNode orNode) {
        return orNode.getPredicateData().getPredicateParameters().stream().map(TermData::getTermName).collect(Collectors.toList());
    }
}
