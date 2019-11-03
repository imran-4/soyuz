package org.apache.flink.datalog.parser.graph;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PrecedenceGraphBuilder extends DatalogBaseVisitor<Graph<Vertex, Edge>> {
	private static Graph<Vertex, Edge> graph = new DefaultDirectedGraph<>(Edge.class);
	private static Vertex rootVertex;
	private static int ruleId = 0;

	@Override
	public Graph<Vertex, Edge> visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		if (ctx.query() == null) {
			return null;
		}
		PredicateData query = new QueryBuilder().visitQuery(ctx.query());
		rootVertex = new QueryVertex(query.getPredicateName(), query);
		graph.addVertex(rootVertex);
		if (ctx.rules() != null) {
			RulesBuilder rulesBuilder = new RulesBuilder();
			List<RuleData> rulesData = rulesBuilder.visitRules(ctx.rules());
			for (RuleData ruleData : rulesData) {
				Vertex ruleVertex = new RuleVertex(String.valueOf(ruleData.getRuleId()), ruleData);
				graph.addVertex(ruleVertex);
			}
			for (Vertex source : graph.vertexSet()) { //doesn't look good, but ok for now...
				List<String> sourcePredicateNames = new ArrayList<>();
				if (source instanceof RuleVertex) {
					sourcePredicateNames = ((RuleVertex) source).getRuleData().getBodyPredicate().stream().map(PredicateData::getPredicateName).collect(Collectors.toList());
				} else if (source instanceof QueryVertex) {
					sourcePredicateNames = List.of(((QueryVertex) source).getTopPredicateData().getPredicateName());
				}
				for (Vertex dest : graph.vertexSet()) {
					if (dest instanceof RuleVertex) {
						String destHeadPredicate = ((RuleVertex) dest).getRuleData().getHeadPredicate().getPredicateName();
						if (sourcePredicateNames.contains(destHeadPredicate)) {
							graph.addEdge(source, dest, new Edge(""));
						}
					}
				}
			}
			System.out.println(graph);
		}
		return graph;
	}

	private static class RulesBuilder extends DatalogBaseVisitor<List<RuleData>> {
		@Override
		public List<RuleData> visitRules(DatalogParser.RulesContext ctx) {
			return ctx.ruleClause().stream().map(ruleClauseContext -> new RuleClauseBuilder().visitRuleClause(ruleClauseContext)).collect(Collectors.toList());
		}
	}

	private static class RuleClauseBuilder extends DatalogBaseVisitor<RuleData> {
		@Override
		public RuleData visitRuleClause(DatalogParser.RuleClauseContext ctx) {
			PredicateData headPredicate = new HeadPredicateBuilder().visit(ctx.headPredicate());
			List<PredicateData> bodyPredicate = new PredicateListBuilder().visitPredicateList(ctx.predicateList());
			RuleData ruleData = new RuleData(ruleId, headPredicate, bodyPredicate);
			ruleId++;
			return ruleData;
		}
	}

	private static class PredicateListBuilder extends DatalogBaseVisitor<List<PredicateData>> {
		@Override
		public List<PredicateData> visitPredicateList(DatalogParser.PredicateListContext ctx) {
			return ctx.predicate().stream().map(predicateContext -> new PredicateBuilder().visitPredicate(predicateContext)).collect(Collectors.toList());
		}
	}

	private static class HeadPredicateBuilder extends DatalogBaseVisitor<PredicateData> {
		@Override
		public PredicateData visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
			return new PredicateBuilder().visitPredicate(ctx.predicate());
		}
	}

	private static class QueryBuilder extends DatalogBaseVisitor<PredicateData> {
		@Override
		public PredicateData visitQuery(DatalogParser.QueryContext ctx) {
			return new PredicateBuilder().visitPredicate(ctx.predicate());
		}
	}

	private static class PredicateBuilder extends DatalogBaseVisitor<PredicateData> {
		@Override
		public PredicateData visitPredicate(DatalogParser.PredicateContext ctx) {
			return new PredicateData(ctx.predicateName().getText(), new TermListBuilder().visitTermList(ctx.termList()));
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
			return new TermData(ctx.getText(), ctx.VARIABLE() != null ? TermData.Adornment.FREE : TermData.Adornment.BOUND);
		}
	}
}
