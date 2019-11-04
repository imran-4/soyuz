package org.apache.flink.datalog.plan.logical;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.parser.tree.Tree;

public abstract class AbstractTreeVisitor<T> implements TreeVisitor<T> {
	public AbstractTreeVisitor() {
	}

	@Override
	public T visit(Tree tree) {
		return tree.accept(this);
	}

	@Override
	public T visitChildren(Node node) {
		return null;
	}
//	@Override
//	public T visitChildren(Node node) {
//		T result = this.defaultResult();
//		int n = node.getChildCount();
//
//		for(int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
//			ParseTree c = node.getChild(i);
//			T childResult = c.accept(this);
//			result = this.aggregateResult(result, childResult);
//		}
//
//		return result;
//	}
//
//	protected T defaultResult() {
//		return null;
//	}
}
