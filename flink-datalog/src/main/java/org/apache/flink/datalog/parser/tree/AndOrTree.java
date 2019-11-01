package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.QueryPredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.TermData;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AndOrTree extends DatalogBaseVisitor<Node> implements Iterator<Node> {
	private static OrNode rootNode = null;
	private static Map<String, Boolean> rulesHead = new LinkedHashMap<>();
	Node tempNode = rootNode;

	@Override
	public OrNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		rulesHead = new LinkedHashMap<>();

		rootNode = new QueryBuilder().visitQuery(ctx.query());
		rootNode.setChildren(new RulesBuilder().visitRules(ctx.rules()));
		System.out.println(rootNode);
		return rootNode;
	}

	@Override
	public boolean hasNext() {

		return false;
	}

	@Override
	public Node next() {
		return rootNode.getChildren().get(0);
	}

	@Override
	public void remove() {
		throw new java.lang.UnsupportedOperationException("Remove operation is not supported.");
	}

	@Override
	public void forEachRemaining(Consumer<? super Node> action) {

	}

	private static class RulesBuilder extends DatalogBaseVisitor<List<AndNode>> {
		@Override
		public List<AndNode> visitRules(DatalogParser.RulesContext ctx) {
			String queryPredicateName = rootNode.getPredicateData().getPredicateName();
			List<AndNode> ruleHeadsMatchingQuery = new ArrayList<>();
			for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
				String headPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();
				if (headPredicateName.equals(queryPredicateName)) {
					ruleHeadsMatchingQuery.add(new RuleClauseBuilder().visitRuleClause(ruleClauseContext));
//					ruleClauseContextIterator.remove();
//					rulesHead.putIfAbsent(headPredicateName, true);
				} else {
					rulesHead.putIfAbsent(headPredicateName, true);
				}
			}
//			for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
//				String headPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();
//				rulesHead.putIfAbsent(headPredicateName, true);
//				if (headPredicateName.equals(queryPredicateName)) {
//					ruleHeadsMatchingQuery.add(new RuleClauseBuilder().visitRuleClause(ruleClauseContext));
//					rulesHead.put(headPredicateName, false);
////					rulesToRemove.add(i);
//					ctx.ruleClause().remove(ruleClauseContext);
//				}
//			}
			return ruleHeadsMatchingQuery;
		}
	}

	private static class RuleClauseBuilder extends DatalogBaseVisitor<AndNode> {
		@Override
		public AndNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
			AndNode headPredicateNode = new HeadPredicateBuilder().visit(ctx.headPredicate());
			headPredicateNode.setChildren(new PredicateListBuilder().visitPredicateList(ctx.predicateList()));
			return headPredicateNode;
		}
	}

	private static class PredicateListBuilder extends DatalogBaseVisitor<List<OrNode>> {
		@Override
		public List<OrNode> visitPredicateList(DatalogParser.PredicateListContext ctx) {
			List<OrNode> ruleBodyNodes = new ArrayList<>();

			for (int i = 0; i < ctx.predicate().size(); i++) {
				OrNode bodyNode = null;

				bodyNode = new PredicateBuilder().visitPredicate(ctx.predicate(i));
				String bodyNodePredicateName = bodyNode.getPredicateData().getPredicateName();

				if (rulesHead.getOrDefault(bodyNodePredicateName, false)) { //todo: may to see if there is a better way to implement the following.
					List<AndNode> ruleHeadsMatchingQuery = new ArrayList<>();
					for (DatalogParser.RuleClauseContext ruleClauseContext : ((DatalogParser.RulesContext) ctx.getParent().getParent()).ruleClause()) {
						String currentRuleClauseHeadPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();
						if (currentRuleClauseHeadPredicateName.equals(bodyNodePredicateName)) {
							ruleHeadsMatchingQuery.add(new RuleClauseBuilder().visitRuleClause(ruleClauseContext));
//							rulesHead.putIfAbsent(currentRuleClauseHeadPredicateName, true);///////todo: not sure.........
						}
						bodyNode.setChildren(ruleHeadsMatchingQuery);
					}
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
			return new AndNode(new SimplePredicateData(ctx.predicate().predicateName().getText(), new TermListBuilder().visitTermList(ctx.predicate().termList())));
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
			return new OrNode(new PrimitivePredicateData(ctx.getText()));
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
