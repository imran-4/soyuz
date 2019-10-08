package org.apache.flink.datalog.parser.tree;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.table.planner.codegen.ExpressionReducer;
import org.apache.flink.table.planner.plan.cost.FlinkCostFactory;

import java.util.ArrayList;
import java.util.List;

public class RelTreeBuilder extends DatalogBaseVisitor<RelNode> {
	FrameworkConfig config;
	RelBuilder builder;

	public RelTreeBuilder(FrameworkConfig config, SchemaPlus plus) {
		SchemaPlus schema = plus; //
		this.config = config; //Frameworks.newConfigBuilder().defaultSchema(schema).build();
		builder = RelBuilder.create(config);


//		SchemaPlus defaultSchema = Frameworks.createRootSchema(true);
//		this.config = Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build();
//		Planner planner = Frameworks.getPlanner(this.config);
//
//		defaultSchema.add("abc", new StreamableTable() {
//			@Override
//			public Table stream() {
//				return null;
//			}
//
//			@Override
//			public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//				RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
//				b.add("v1", typeFactory.createJavaType(String.class));
//				b.add("v2", typeFactory.createJavaType(String.class));
//				return b.build();
//			}
//
//			@Override
//			public Statistic getStatistic() {
//				return null;
//			}
//
//			@Override
//			public Schema.TableType getJdbcTableType() {
//				return null;
//			}
//
//			@Override
//			public boolean isRolledUp(String s) {
//				return false;
//			}
//
//			@Override
//			public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode, CalciteConnectionConfig calciteConnectionConfig) {
//				return false;
//			}
//		});
//
//		this.builder = RelBuilder.create(this.config);

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
