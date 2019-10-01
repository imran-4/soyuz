package org.apache.flink.datalog.tree;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;

public class AstBuilder extends DatalogBaseVisitor<AST> {

	@Override
	public AST visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitDatabase(DatalogParser.DatabaseContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitSchema(DatalogParser.SchemaContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitTableName(DatalogParser.TableNameContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitColumnsList(DatalogParser.ColumnsListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitColumnName(DatalogParser.ColumnNameContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitColumnDataType(DatalogParser.ColumnDataTypeContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		AST ast =  new RuleAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitFact(DatalogParser.FactContext ctx) {
		AST ast = new FactAST();
		ast.addChild(visitChildren(ctx));

		return ast;
	}

	@Override
	public AST visitConstantList(DatalogParser.ConstantListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitQuery(DatalogParser.QueryContext ctx) {

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
		AST ast = new PredicateAST();
		ast.addChild(visitChildren(ctx));
		return ast;
	}

	@Override
	public AST visitPredicateName(DatalogParser.PredicateNameContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitTermList(DatalogParser.TermListContext ctx) {
		return visitChildren(ctx);
	}

	@Override
	public AST visitTerm(DatalogParser.TermContext ctx) {
		AST ast = new TermAST();
		ast.addChild(visitChildren(ctx));
		return ast;
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
