package org.apache.flink.datalog.tree;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.DelegatingSchema;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;

import java.util.ArrayList;
import java.util.List;

public class RelTreeBuilder extends DatalogBaseVisitor<RelNode> {
	FrameworkConfig config;
	RelBuilder builder;

	public RelTreeBuilder() {
		SchemaPlus schema = Frameworks.createRootSchema(true);
		config = Frameworks.newConfigBuilder().defaultSchema(schema).build();

		builder = RelBuilder.create(config);
	}

	@Override
	public RelNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		System.out.println("Inside visitCompileUnit" + ctx.getText());

		for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
			visit(ruleClauseContext);
		}
		System.out.println(RelOptUtil.toString(builder.build()));

		return null;
	}

	@Override
	public RelNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		System.out.println("Inside visitRuleClause" + ctx.getText());
		builder.push(visit(ctx.headPredicate()));
		builder.push(visit(ctx.predicateList()));
		System.out.println(RelOptUtil.toString(builder.build()));

		return builder.build();
	}

	@Override
	public RelNode visitPredicateList(DatalogParser.PredicateListContext ctx) {
		System.out.println("Inside visitPRedicateList" + ctx.getText());
		List<RelBuilder> builders = new ArrayList<>();

		for (DatalogParser.PredicateContext predicateContext : ctx.predicate()) {
			builder.push(visit(predicateContext));
		}
		System.out.println(RelOptUtil.toString(builder.build()));

		return builder.build();
	}

	@Override
	public RelNode visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
		return visit(ctx.predicate());
	}


	@Override
	public RelNode visitPredicate(DatalogParser.PredicateContext ctx) {
		System.out.println("Inside visitPRedicateList" + ctx.getText());
		String predicateName = ctx.predicateName().getText();
		return builder
			.scan(predicateName)
			.push(visit(ctx.termList()))
			.build();
	}

	@Override
	public RelNode visitTermList(DatalogParser.TermListContext ctx) {
		for (DatalogParser.TermContext termContext : ctx.term()) {
			builder.push(visitChildren(termContext));
		}
		System.out.println(RelOptUtil.toString(builder.build()));
		return builder.build();
	}


	@Override
	public RelNode visitTerm(DatalogParser.TermContext ctx) {
		return builder.project(builder.field(ctx.getText())).build();
	}

	@Override
	public RelNode visitAtom(DatalogParser.AtomContext ctx) {
		return builder.project(builder.field(ctx.getText())).build();
	}
}
