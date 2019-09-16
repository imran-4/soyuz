package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;

public class AstBuilder extends DatalogBaseVisitor<Object> {

	@Override
	public Object visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitSchema(DatalogParser.SchemaContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitColumnsList(DatalogParser.ColumnsListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitFact(DatalogParser.FactContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitConstantList(DatalogParser.ConstantListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitQuery(DatalogParser.QueryContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitRetraction(DatalogParser.RetractionContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitPredicateList(DatalogParser.PredicateListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitNotPredicate(DatalogParser.NotPredicateContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitPrimitivePredicate(DatalogParser.PrimitivePredicateContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitPredicate(DatalogParser.PredicateContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitTermList(DatalogParser.TermListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitTerm(DatalogParser.TermContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitAtom(DatalogParser.AtomContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public Object visitInteger(DatalogParser.IntegerContext ctx) {
		return visitChildren(ctx);
	}
}
