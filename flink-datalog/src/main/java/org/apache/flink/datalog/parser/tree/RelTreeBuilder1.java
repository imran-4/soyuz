package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.graph.Edge;
import org.apache.flink.datalog.parser.tree.graph.PredicateData;
import org.apache.flink.datalog.parser.tree.graph.RuleData;
import org.apache.flink.datalog.parser.tree.graph.Vertex;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.calcite.FlinkRelBuilder;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RelTreeBuilder1 extends DatalogBaseVisitor { //may be we need to use FlinkRelBuilder instead of RelNode
	private FlinkRelBuilder relBuilder;
	private String currentCatalog;
	private String currentDatabase;
	private TableEnvironment environment;

	Graph<Vertex, Edge> graph;

	public RelTreeBuilder1() {
		graph = new DefaultDirectedGraph<>(Edge.class);
	}

	// DO WE NEED TO IMPLEMENT SEMI NAIVE EVALUATION HERE..
	@Override
	public Graph<Vertex, Edge> visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		if (ctx.query() != null) {
//			return visit(ctx.query());
		} else if (ctx.rules() != null) {
//			return visit(ctx.rules());
		} else
			return null;
		return null;
	}

	@Override
	public List<RuleData> visitRules(DatalogParser.RulesContext ctx) {
		return ctx.ruleClause().stream().map(this::visitRuleClause).collect(Collectors.toList());
	}

	@Override
	public RuleData visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		Vertex vertex = new Vertex(ctx.getText(), new RuleData(1, visitHeadPredicate(ctx.headPredicate()), visitPredicateList(ctx.predicateList())));
		graph.addVertex(vertex);
		return null;
	}

	@Override
	public List<PredicateData> visitPredicateList(DatalogParser.PredicateListContext ctx) {
		return ctx.predicate().stream().map(this::visitPredicate).collect(Collectors.toList());
	}

	@Override
	public PredicateData visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
		return visitPredicate(ctx.predicate());
	}

	@Override
	public PredicateData visitQuery(DatalogParser.QueryContext ctx) {
		return visitPredicate(ctx.predicate());
	}

	@Override
	public PredicateData visitPredicate(DatalogParser.PredicateContext ctx) {
		return new PredicateData(ctx.predicateName().getText(), visitTermList(ctx.termList()));

	}

	@Override
	public List<String> visitTermList(DatalogParser.TermListContext ctx) {
		return ctx.term().stream().map(this::visitTerm).collect(Collectors.toList());
	}

	@Override
	public String visitTerm(DatalogParser.TermContext ctx) {
		return ctx.getText();
	}
}
