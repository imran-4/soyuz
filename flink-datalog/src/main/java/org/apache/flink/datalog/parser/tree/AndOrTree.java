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

package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.QueryPredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.TermData;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class AndOrTree extends DatalogBaseVisitor<Node> {

	@Override
	public OrNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		OrNode rootNode = new QueryBuilder().visitQuery(ctx.query());
		rootNode.setChildren(new RulesBuilder(rootNode, null).visitRules(ctx.rules()));
		return rootNode;
	}

	private static class RulesBuilder extends DatalogBaseVisitor<List<AndNode>> {
		private OrNode currentNode;
		private AndNode parentNode; //in case of root node, this is null

		RulesBuilder(OrNode currentNode, AndNode parentNode) {
			this.currentNode = currentNode;
			this.parentNode = parentNode;
		}

		@Override
		public List<AndNode> visitRules(DatalogParser.RulesContext ctx) {
			String queryPredicateName = currentNode.getPredicateData().getPredicateName();
			List<AndNode> ruleHeadsMatchingQuery = new ArrayList<>();
			for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
				String headPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();

				if (headPredicateName.equals(queryPredicateName)) {
					if (currentNode.getPredicateData() instanceof SimplePredicateData) {
						((SimplePredicateData) currentNode.getPredicateData()).setIdb(true);
					}
					if (parentNode != null) {
						if (parentNode.getPredicateData().getPredicateName().equals(queryPredicateName)) {
							continue;
						}
					}
					ruleHeadsMatchingQuery.add(new RuleClauseBuilder(ctx).visitRuleClause(ruleClauseContext));
				}
			}
			return ruleHeadsMatchingQuery;
		}
	}

	private static class RuleClauseBuilder extends DatalogBaseVisitor<AndNode> {
		DatalogParser.RulesContext rulesContext;

		RuleClauseBuilder(DatalogParser.RulesContext rulesContext) {
			this.rulesContext = rulesContext;
		}

		@Override
		public AndNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
			DatalogParser.HeadPredicateContext headPredicateContext = ctx.headPredicate();
			AndNode headPredicateNode = new HeadPredicateBuilder().visit(headPredicateContext);
			headPredicateNode.setChildren(new PredicateListBuilder(headPredicateNode, rulesContext).visitPredicateList(ctx.predicateList()));
			return headPredicateNode;
		}
	}

	private static class PredicateListBuilder extends DatalogBaseVisitor<List<OrNode>> {
		DatalogParser.RulesContext rulesContext;
		AndNode headPredicateNode;

		PredicateListBuilder(AndNode headPredicateContext, DatalogParser.RulesContext rulesContext) {
			this.headPredicateNode = headPredicateContext;
			this.rulesContext = rulesContext;
		}

		@Override
		public List<OrNode> visitPredicateList(DatalogParser.PredicateListContext ctx) {
			List<OrNode> ruleBodyNodes = new ArrayList<>();

			for (int i = 0; i < ctx.predicate().size(); i++) {
				OrNode bodyNode = null;

				bodyNode = new PredicateBuilder().visitPredicate(ctx.predicate(i));
				if (bodyNode.getPredicateData().getPredicateName().equals(headPredicateNode.getPredicateData().getPredicateName())) {
					headPredicateNode.setRecursive(true);
				}
				List<AndNode> subNodes = new RulesBuilder(bodyNode, this.headPredicateNode).visitRules((DatalogParser.RulesContext) ctx.getParent().getParent()); //for some nodes it would be an additional step (which can be avoided by storing headnodes in a map. but i didnt want to consume memory on that.)
				if (subNodes.size() > 0) {
					bodyNode.setChildren(subNodes);
				}
				ruleBodyNodes.add(bodyNode);
			}

			for (int i = 0; i < ctx.primitivePredicate().size(); i++) {
				OrNode bodyNode = null;

				bodyNode = new PrimitivePredicateBuilder().visitPrimitivePredicate(ctx.primitivePredicate(i));
				ruleBodyNodes.add(bodyNode);
			}
			for (int i = 0; i < ctx.notPredicate().size(); i++) {
				throw new UnsupportedOperationException("Not operator is not supported yet.");
			}
			return ruleBodyNodes;
		}
	}

	private static class HeadPredicateBuilder extends DatalogBaseVisitor<AndNode> {
		@Override
		public AndNode visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
			return new AndNode(new SimplePredicateData(ctx.predicate().predicateName().getText(), new TermListBuilder().visitTermList(ctx.predicate().termList()), true));
		}
	}

	private static class QueryBuilder extends DatalogBaseVisitor<OrNode> {
		@Override
		public OrNode visitQuery(DatalogParser.QueryContext ctx) {
			return new OrNode(new QueryPredicateData(ctx.predicate().predicateName().getText(), new TermListBuilder().visitTermList(ctx.predicate().termList()))); //implemented separately, because we may need to set other parameters as well, otherwise visit predicate and return.
		}
	}

	private static class PrimitivePredicateBuilder extends DatalogBaseVisitor<OrNode> {
		@Override
		public OrNode visitPrimitivePredicate(DatalogParser.PrimitivePredicateContext ctx) {
			TermData leftTerm = null, rightTerm = null;
			if (ctx.CONSTANT(0) != null) {
				leftTerm = new TermData(ctx.CONSTANT(0).getText(), TermData.Adornment.BOUND);
			} else if (ctx.VARIABLE(0) != null) {
				leftTerm = new TermData(ctx.VARIABLE(0).getText(), TermData.Adornment.FREE);
			} else if (ctx.DECIMAL(0) != null) {
				leftTerm = new TermData(ctx.DECIMAL(0).getText(), TermData.Adornment.BOUND);
			}

			if (ctx.CONSTANT(1) != null) {
				rightTerm = new TermData(ctx.CONSTANT(1).getText(), TermData.Adornment.BOUND);
			} else if (ctx.VARIABLE(1) != null) {
				rightTerm = new TermData(ctx.VARIABLE(1).getText(), TermData.Adornment.FREE);
			} else if (ctx.DECIMAL(1) != null) {
				rightTerm = new TermData(ctx.DECIMAL(1).getText(), TermData.Adornment.BOUND);
			}

			return new OrNode(new PrimitivePredicateData(leftTerm, ctx.COMPARISON_OPERATOR().getText(), rightTerm));
		}
	}

	private static class PredicateBuilder extends DatalogBaseVisitor<OrNode> {
		@Override
		public OrNode visitPredicate(DatalogParser.PredicateContext ctx) {
			return new OrNode(new SimplePredicateData(ctx.predicateName().getText(), new TermListBuilder().visitTermList(ctx.termList())));
		}
	}

	private static class TermListBuilder extends DatalogBaseVisitor<List<TermData>> {
		@Override
		public List<TermData> visitTermList(DatalogParser.TermListContext ctx) {
			return ctx.term().stream().map(termContext -> new TermBuilder().visitTerm(termContext)).collect(Collectors.toList());
		}
	}

	private static class TermBuilder extends DatalogBaseVisitor<TermData> {
		@Override
		public TermData visitTerm(DatalogParser.TermContext ctx) {
			if (ctx.CONSTANT() != null) { //todo: there are lots of other cases that needs to be covered, but so far these two are enough.
				return new TermData(ctx.getText(), TermData.Adornment.BOUND);
			} else {
				return new TermData(ctx.getText(), TermData.Adornment.FREE);
			}
		}
	}
}
