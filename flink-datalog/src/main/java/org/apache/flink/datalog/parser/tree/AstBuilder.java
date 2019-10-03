package org.apache.flink.datalog.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;

public class AstBuilder extends DatalogBaseVisitor<AST> {

	@Override
	public AST visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		System.out.println("Inside visitCompileUnit" + ctx.getText());
//		AST ast = new ProgramAST();
//		ast.addChild(visitChildren(ctx));

		return visitChildren(ctx);
	}

	@Override
	public AST visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		System.out.println("Inside visitRuleClause" + ctx.getText());
		AST ast = new RuleAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitPredicateList(DatalogParser.PredicateListContext ctx) {
		System.out.println("Inside visitPredicateList" + ctx.getText());
		/*
		predicateList
    		: ( predicate | notPredicate | primitivePredicate ) ( COMMA ( predicate | notPredicate | primitivePredicate ) )*
    		;
		*/
		AST ast = new PredicateListAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitNotPredicate(DatalogParser.NotPredicateContext ctx) {
		System.out.println("Inside visitNotPredicate" + ctx.getText());
		return visitChildren(ctx);
	}

	@Override
	public AST visitPrimitivePredicate(DatalogParser.PrimitivePredicateContext ctx) {
		System.out.println("Inside visitPrimitivePredicate" + ctx.getText());
		return visitChildren(ctx);
	}

	@Override
	public AST visitPredicate(DatalogParser.PredicateContext ctx) {
		System.out.println("Inside visitPredicate" + ctx.getText());
		AST ast = new PredicateAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitPredicateName(DatalogParser.PredicateNameContext ctx) {
		System.out.println("Inside visitPredicateName" + ctx.getText());
		return visitChildren(ctx);
	}

	@Override
	public AST visitTermList(DatalogParser.TermListContext ctx) {
		System.out.println("Inside visitTermList" + ctx.getText());

		AST ast = new TermListAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitTerm(DatalogParser.TermContext ctx) {
		System.out.println("Inside visitTerm" + ctx.getText());
		AST ast = new TermAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitAtom(DatalogParser.AtomContext ctx) {
		System.out.println("Inside visitAtom" + ctx.getText());
		return visitChildren(ctx);
	}

	@Override
	public AST visitInteger(DatalogParser.IntegerContext ctx) {
		System.out.println("Inside visitInteger" + ctx.getText());
		return visitChildren(ctx);
	}
}
