package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.tree.TreeNode;
import org.apache.flink.datalog.tree.VariableNode;


public class AstBuilder extends DatalogBaseVisitor<TreeNode> {

	@Override
	public TreeNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		visit(ctx.fact(0));
		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitColumnsList(DatalogParser.ColumnsListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitFact(DatalogParser.FactContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitConstantList(DatalogParser.ConstantListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitRetraction(DatalogParser.RetractionContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitPredicateList(DatalogParser.PredicateListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitNotPredicate(DatalogParser.NotPredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitPrimitivePredicate(DatalogParser.PrimitivePredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitPredicate(DatalogParser.PredicateContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitTermList(DatalogParser.TermListContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitTerm(DatalogParser.TermContext ctx) {

		TreeNode node = new VariableNode();

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitAtom(DatalogParser.AtomContext ctx) {

		return visitChildren(ctx);
	}

	@Override
	public TreeNode visitInteger(DatalogParser.IntegerContext ctx) {

		return visitChildren(ctx);
	}
}
