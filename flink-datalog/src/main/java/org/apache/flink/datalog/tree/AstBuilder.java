package org.apache.flink.datalog.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;

public class AstBuilder extends DatalogBaseVisitor<AST> {

	@Override
	public AST visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		visit(ctx.fact(0));
		return visitChildren(ctx);
	}

	@Override
	public AST visitColumnsList(DatalogParser.ColumnsListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitRuleClause(DatalogParser.RuleClauseContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitFact(DatalogParser.FactContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitConstantList(DatalogParser.ConstantListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitRetraction(DatalogParser.RetractionContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitPredicateList(DatalogParser.PredicateListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitNotPredicate(DatalogParser.NotPredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitPrimitivePredicate(DatalogParser.PrimitivePredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitPredicate(DatalogParser.PredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitTermList(DatalogParser.TermListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitTerm(DatalogParser.TermContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitAtom(DatalogParser.AtomContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public AST visitInteger(DatalogParser.IntegerContext ctx) {

		return visitChildren(ctx);
	}
}
