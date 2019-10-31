package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.predicate.PrimitivePredicateData;
import org.apache.flink.datalog.parser.tree.predicate.QueryPredicateData;
import org.apache.flink.datalog.parser.tree.predicate.SimplePredicateData;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AndOrTree extends DatalogBaseVisitor<Node> implements Iterator<Node> {
	private static OrNode rootNode = null;
	private static Map<String, Boolean> rulesHead = new LinkedHashMap<>();

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
		return null;
	}

	@Override
	public void remove() {

	}

	@Override
	public void forEachRemaining(Consumer<? super Node> action) {

	}

	private static class RulesBuilder extends DatalogBaseVisitor<List<AndNode>> {
		@Override
		public List<AndNode> visitRules(DatalogParser.RulesContext ctx) {
			String queryPredicateName = rootNode.getPredicateData().getPredicateName();
			List<AndNode> ruleHeadsMatchingQuery = new ArrayList<>();
			Iterator<DatalogParser.RuleClauseContext> ruleClauseContextIterator = ctx.ruleClause().iterator();
			while (ruleClauseContextIterator.hasNext()) {
				DatalogParser.RuleClauseContext ruleClauseContext = ruleClauseContextIterator.next();
				String headPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();
				if (headPredicateName.equals(queryPredicateName)) {
					ruleHeadsMatchingQuery.add(new RuleClauseBuilder().visitRuleClause(ruleClauseContext));
//					ruleClauseContextIterator.remove();
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
				if (ctx.predicate(i) != null) {
					bodyNode = new PredicateBuilder().visitPredicate(ctx.predicate(i));
					String bodyNodePredicateName = bodyNode.getPredicateData().getPredicateName();

					if (rulesHead.getOrDefault(bodyNodePredicateName, false)) {
						List<AndNode> ruleHeadsMatchingQuery = new ArrayList<>();
						for (DatalogParser.RuleClauseContext ruleClauseContext: ((DatalogParser.RulesContext)ctx.getParent().getParent()).ruleClause()) {
							String currentRuleClauseHeadPredicateName = ruleClauseContext.headPredicate().predicate().predicateName().getText();
							if (currentRuleClauseHeadPredicateName.equals(bodyNodePredicateName)) {
								ruleHeadsMatchingQuery.add(new RuleClauseBuilder().visitRuleClause(ruleClauseContext));
							}
							bodyNode.setChildren(ruleHeadsMatchingQuery);
						}
					}

				} else {
					bodyNode = new PrimitivePredicateBuilder().visitPrimitivePredicate(ctx.primitivePredicate(i));
				}
				ruleBodyNodes.add(bodyNode);
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

	//--------------------------------------------------------------------

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

	private static class TermListBuilder extends DatalogBaseVisitor<List<String>> {
		@Override
		public List<String> visitTermList(DatalogParser.TermListContext ctx) {
			return ctx.term().stream().map(termContext -> new TermBuilder().visitTerm(termContext)).collect(Collectors.toList());
		}
	}

	private static class TermBuilder extends DatalogBaseVisitor<String> {
		@Override
		public String visitTerm(DatalogParser.TermContext ctx) {
			return ctx.getText();
		}
	}
}
