package org.apache.flink.datalog.parser.tree.graph;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphBuilder extends DatalogBaseVisitor<Graph<Vertex, Edge>> {
	private static Graph<Vertex, Edge> graph = new DefaultDirectedGraph<>(Edge.class);
	private static Map<String, List<String>>  rulePredicateMapping = new HashMap<>(); //predicate_name,rule_id
	private static int ruleId = 0;

	@Override
	public Graph<Vertex, Edge> visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		if (ctx.query() != null) {
//			return visit(ctx.query());
		} else if (ctx.rules() != null) {
			RulesBuilder rulesBuilder = new RulesBuilder();
			List<RuleData> rulesData = rulesBuilder.visitRules(ctx.rules());
			for (RuleData ruleData : rulesData) {
				graph.addVertex(new Vertex(String.valueOf(ruleData.getRuleId()), ruleData));
			}

			for (Vertex source : graph.vertexSet()) {
//				String sourceHeadPredicateName = source.getRuleData().getHeadPredicate().getPredicateName();
				List<String> sourceBodyPredicateNames = source.getRuleData().getBodyPredicate().stream().map(PredicateData::getPredicateName).collect(Collectors.toList());
				for (Vertex dest : graph.vertexSet()) {
					String destHeadPredicate = dest.getRuleData().getHeadPredicate().getPredicateName();
//					List<String> destBodyPredicates = dest.getRuleData().getBodyPredicate().stream().map(PredicateData::getPredicateName).collect(Collectors.toList());
					if (sourceBodyPredicateNames.contains(destHeadPredicate)) {
						graph.addEdge(source, dest, new Edge(""));
					}
				}
			}
			System.out.println(graph);
		} else
			return null;
		return null;
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
//			Vertex vertex = new Vertex(String.valueOf(ruleId), ruleData);
//			graph.addVertex(vertex);
//
//			for (PredicateData predicate : bodyPredicate) {
//				String predicateName = predicate.getPredicateName();
//				List<String> predicateValue = rulePredicateMapping.get(predicateName);
//				if (predicateValue == null)
//					predicateValue = new ArrayList<>();
//				else {
//					predicateValue.add(String.valueOf(ruleId));
//				}
//				rulePredicateMapping.put(predicateName, predicateValue);
//			}
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

