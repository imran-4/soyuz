package org.apache.flink.datalog.parser.tree;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang.math.IntRange;
import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.CatalogManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//Todo: return list of plans (RelNode) --depends on the input programs

public class RelTreeBuilder extends DatalogBaseVisitor<RelNode> {
	private FrameworkConfig config;
	private RelOptCluster cluster;
	private CatalogManager catalogManager;
	private String currentCatalog;
	private String currentDatabase;

	private FlinkRelBuilder relBuilder;
	public RelTreeBuilder(FlinkRelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	// DO WE NEED TO IMPLEMENT SEMI NAIVE EVALUATION HERE..
	@Override
	public RelNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		System.out.println("Inside visitCompileUnit" + ctx.getText());
		if (ctx.query() != null) {
			return visit(ctx.query());
		} else if (ctx.rules() != null) {
			return visit(ctx.rules());
		} else
			return null;
	}

	@Override
	public RelNode visitRules(DatalogParser.RulesContext ctx) {
		for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
			// here find a rule without IDB (or do it using predicate connection graph)
		}
		return null;
	}

	@Override
	public RelNode visitQuery(DatalogParser.QueryContext ctx) {
		return visit(ctx.predicate()); // a query is just a predicate ending with ?
	}

	@Override
	public RelNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		System.out.println("Inside visitRuleClause" + ctx.getText());

		//take unions
		relBuilder.push((visit(ctx.predicateList())));
		relBuilder.push((visit(ctx.headPredicate()))); //use transient scan

		String headPredicateName = "abc";
		relBuilder.repeatUnion(headPredicateName, true);

		System.out.println(RelOptUtil.toString(relBuilder.build()));
		return relBuilder.build();
	}

	@Override
	public RelNode visitPredicateList(DatalogParser.PredicateListContext ctx) {
		System.out.println("Inside visitPRedicateList" + ctx.getText());
		List<RelNode> nodes = new ArrayList<>();
		int numberOfPredicates = ctx.predicate().size();
		for (DatalogParser.PredicateContext predicateContext : ctx.predicate()) {
			nodes.add(visit(predicateContext));
		}
//		builder.union(true); //union most recent added operations
//		RelDataTypeFactory factory = new FlinkTypeFactory(RelDataTypeSystem.DEFAULT);
//		RexBuilder rexBuilder = new RexBuilder(factory);
//		rexBuilder.makeLiteral("");
		if (numberOfPredicates > 1) { //join based on the predicate type.
//			builder.join(JoinRelType.FULL, rexBuilder.);
		}
		System.out.println(RelOptUtil.toString(relBuilder.build()));
		return relBuilder.build();
	}

	@Override
	public RelNode visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
		// head predicates are IDBs.
		return relBuilder.transientScan(ctx.predicate().predicateName().getText()).project().build();
	}

	@Override
	public RelNode visitPredicate(DatalogParser.PredicateContext ctx) {
		String predicateName = ctx.predicateName().getText();
		this.currentCatalog = "my_catalog";
		this.currentDatabase = "my_database";
		List<RexNode> filters = new ArrayList<>();
		relBuilder.clear();
		relBuilder.scan(this.currentCatalog, this.currentDatabase, predicateName);
		int i = 0;
		for (DatalogParser.TermContext termContext : ctx.termList().term()) {
			if (termContext.CONSTANT() != null) {
				filters.add(relBuilder.call(SqlStdOperatorTable.EQUALS,
					relBuilder.field(i),
					relBuilder.literal(termContext.CONSTANT().getText())));
			}
			i++;
		}
		if (filters.size() == 1)
			relBuilder.filter(filters.get(0));
		else if (filters.size()  > 1)
			relBuilder.filter(relBuilder.call(SqlStdOperatorTable.AND, filters));

		relBuilder.project(relBuilder.fields(IntStream.range(0, ctx.termList().term().size()).boxed().collect(Collectors.toList())));
		RelNode queryNode = relBuilder.build();
		System.out.println(RelOptUtil.toString(queryNode));
		return queryNode;
	}
}
