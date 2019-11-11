package org.apache.flink.datalog.parser.tree;

import org.antlr.v4.runtime.tree.RuleNode;

public abstract class AbstractTreeVisitor<T> implements TreeVisitor<T> {
	public AbstractTreeVisitor() {
	}

	@Override
	public T visit(Tree tree) {
		return tree.accept(this);
	}

	@Override
	public T visitChildren(Node node) {
		T result = this.defaultResult();
		for(int i = 0; i < node.getChildCount(); i++) {
			Node c = node.getChild(i);
			result = c.accept(this);
		}
		return result;
	}

	protected T defaultResult() {
		return null;
	}
}
