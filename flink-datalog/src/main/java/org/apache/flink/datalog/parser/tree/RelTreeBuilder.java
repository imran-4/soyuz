package org.apache.flink.datalog.parser.tree;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import java.util.ArrayList;
import java.util.List;

//Todo: return list of plans (RelNode) --depends on the input programs

public class RelTreeBuilder extends DatalogBaseVisitor<RelNode> {
	private RelBuilder builder;
	private FrameworkConfig config;
	private CatalogManager catalogManager;
	private String currentCatalog;
	private String currentDatabase;

	public RelTreeBuilder(FrameworkConfig config, CatalogManager catalogManager) {
		this.config = config;
		builder = RelBuilder.create(config);
		this.catalogManager = catalogManager;
		this.currentCatalog = catalogManager.getCurrentCatalog();
		this.currentDatabase = catalogManager.getCurrentDatabase();
	}

	// DO WE NEED TO IMPLEMENT SEMI NAIVE EVALUATION HERE..
	@Override
	public RelNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		System.out.println("Inside visitCompileUnit" + ctx.getText());
		if (ctx.query() != null) {
			return visit(ctx.query());
		} else if (ctx.ruleClause().size() > 0) {
			//todo:...
			for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
				visit(ruleClauseContext);
			}
			return null;
		}
		System.out.println(RelOptUtil.toString(builder.build()));
		return null;
	}

	@Override
	public RelNode visitQuery(DatalogParser.QueryContext ctx) {
		builder.clear();
		String predicateName = ctx.predicate().predicateName().getText();
		builder.scan(this.currentCatalog, this.currentDatabase, predicateName);
		List<RexNode> filters = new ArrayList<>();
		List<RexNode> fields = new ArrayList<>();
		int i = 1;
		for (DatalogParser.TermContext termContext : ctx.predicate().termList().term()) {
			if (termContext.CONSTANT() != null) {  // or other types that are not VARIABLE()
				filters.add(builder.call(SqlStdOperatorTable.EQUALS,
					builder.field(i),
					builder.literal(termContext.CONSTANT().getText())));
			}
			fields.add(builder.field(i)); // add all fields for the projection
			i++;
		}
		builder.clear();
		builder
			.scan(this.currentCatalog, this.currentDatabase,predicateName)
			.filter(builder.call(SqlStdOperatorTable.AND, filters))
			.project(fields);
		System.out.println(RelOptUtil.toString(builder.build()));
		return builder.build();
	}

	@Override
	public RelNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		System.out.println("Inside visitRuleClause" + ctx.getText());

		//take unions
		builder.push(visit(ctx.predicateList()));
		builder.push(visit(ctx.headPredicate())); //use transient scan

		String headPredicateName = "abc";
		builder.repeatUnion(headPredicateName, true);

		System.out.println(RelOptUtil.toString(builder.build()));
		return builder.build();
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
		RelDataTypeFactory factory = new FlinkTypeFactory(RelDataTypeSystem.DEFAULT);
		RexBuilder rexBuilder = new RexBuilder(factory);
		rexBuilder.makeLiteral("");
		if (numberOfPredicates > 1) { //join based on the predicate type.
//			builder.join(JoinRelType.FULL, rexBuilder.);
		}
		System.out.println(RelOptUtil.toString(builder.build()));
		return builder.build();
	}

	@Override
	public RelNode visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
		return builder.transientScan(ctx.predicate().predicateName().getText()).project().build();
	}

	@Override
	public RelNode visitPredicate(DatalogParser.PredicateContext ctx) {
		System.out.println("Inside visitPredicateList" + ctx.getText());
		String predicateName = ctx.predicateName().getText();
		List<RexNode> rexNodes = new ArrayList<>();
		for (DatalogParser.TermContext termContext : ctx.termList().term()) {
//			rexNodes.add(visit(termContext));
		}
		return builder.scan(this.currentCatalog, this.currentDatabase, predicateName).project(rexNodes.toArray(new RexNode[0])).build();   //todo: project might have variables or constants..
	}

//	@Override
//	public RelNode visitTermList(DatalogParser.TermListContext ctx) {
//		for (DatalogParser.TermContext termContext : ctx.term()) {
//			builder.push(visitChildren(termContext));
//		}
//		System.out.println(RelOptUtil.toString(builder.build()));
//		return builder.build();
//	}
//
//	@Override
//	public RelNode visitTerm(DatalogParser.TermContext ctx) {
//		return builder.field("","");
//	}
//
//	@Override
//	public RelNode visitAtom(DatalogParser.AtomContext ctx) {
//		return builder.project(builder.field(ctx.getText())).build();
//	}
}
